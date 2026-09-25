"""BayesWave Pipeline specification for Asimov."""

import configparser
import glob
import os
import shutil
import subprocess
from shutil import copyfile, copytree

import numpy as np
from asimov import config
from asimov.git import AsimovFileNotFound
from asimov.pipeline import Pipeline, PipelineException
from asimov.storage import AlreadyPresentException, Store
from asimov.utils import set_directory


class BayesWave(Pipeline):
    """
    The BayesWave Pipeline integration for Asimov.

    This class provides an interface between Asimov and the BayesWave
    parameter estimation pipeline, handling DAG file creation, job
    submission, and PSD collection.

    Parameters
    ----------
    production : :class:`asimov.Production`
       The production object.
    category : str, optional
        The category of the job.
        Defaults to "analyses".

    Attributes
    ----------
    name : str
        The name of the pipeline ("BayesWave")
    STATUS : set
        Set of possible job statuses

    Examples
    --------
    >>> from asimov_bayeswave import BayesWave
    >>> pipeline = BayesWave(production)
    >>> pipeline.build_dag()
    >>> pipeline.submit_dag()
    """

    name = "BayesWave"
    STATUS = {"wait", "stuck", "stopped", "running", "finished"}

    # Vocabulary accepted for likelihood.components.signal / .glitch / and
    # .noise.psd (asimov v0.8-preview asimov/vocabulary.yaml). "cbc" and
    # "fixed" are recognised but not yet implemented by this plugin -- see
    # _components() below, which raises a clear PipelineException for them
    # rather than silently ignoring or mis-running them.
    _VALID_SIGNAL_COMPONENTS = {"none", "wavelets", "chirplets", "cbc"}
    _VALID_GLITCH_COMPONENTS = {"none", "wavelets", "chirplets"}
    _VALID_NOISE_PSD = {"fit", "fixed"}

    # Maps the resolved run mode (see _components()) to the BayesWave model
    # -selection flag which restricts BayesWave/BayesWavePost to that
    # combination of models. Verified against BayesWaveIO.c
    # parse_command_line() (shared by both BayesWave and BayesWavePost) in
    # the BayesWave source (git.ligo.org/lscsoft/bayeswave):
    #   --cleanOnly  -> noise/glitch/signal/full models off, clean model
    #                   stays on (the default) -- today's PSD-only mode.
    #   --signalOnly -> noise/glitch/full off, signal on; clean stays on.
    #   --glitchOnly -> noise/signal/full off, glitch on; clean stays on.
    #   --fullOnly   -> noise/glitch/signal off, only the *combined*
    #                   "full" (signal+glitch) model runs (see
    #                   BayesWaveIO.c:1858-1865). This is NOT what a
    #                   coherence test needs: BayesWave.c's evidence
    #                   fprintf()s for "signal"/"glitch"/"noise" run
    #                   unconditionally regardless of which models were
    #                   actually sampled (see BayesWave.c ~1157-1240), so a
    #                   --fullOnly run's evidence.dat has real numbers only
    #                   for "full" -- the "signal"/"glitch"/"noise" lines
    #                   are placeholder zeros from models that never ran,
    #                   not real per-model evidences, and there is no
    #                   signal:glitch Bayes factor to be had.
    #   (no flag)    -> BayesWave's own defaults (BayesWaveIO.c
    #                   ~1638-1649): clean, noise, glitch AND signal all
    #                   on, full off. This runs signal, glitch and noise as
    #                   genuinely separate RJMCMC phases with real
    #                   evidences each, which is what a coherence test
    #                   (comparing signal/glitch/noise evidence) actually
    #                   needs -- see the "coherence" mode below.
    # In every case the "clean" model (used for on-source PSD estimation)
    # is left at its default of "on", so PSD collection keeps working
    # exactly as it does today regardless of which other models are
    # requested.
    _MODE_FLAG = {
        "psd": "cleanOnly",
        "signal": "signalOnly",
        "glitch": "glitchOnly",
        "full": "fullOnly",
        # No restriction flag: BayesWave's own defaults apply.
        "coherence": None,
    }

    def __init__(self, production, category=None):
        super(BayesWave, self).__init__(production, category)
        self.logger.info("Using the Bayeswave pipeline plugin")
        if not production.pipeline.lower() == "bayeswave":
            raise PipelineException("Pipeline mismatch")

        try:
            self.category = config.get("general", "calibration_directory")
        except configparser.NoOptionError:
            self.category = "analyses"
            self.logger.info("Assuming analyses directory.")

    @property
    def config_template(self):
        """
        The path to the bundled Liquid ini template for this pipeline.

        asimov's ``manage build`` step falls back to this property to
        render a production's ini when one doesn't already exist in the
        event repository (see ``Analysis.make_config``). Without this,
        asimov would fall through to looking for
        ``asimov/configs/bayeswave.ini`` inside asimov's own package,
        which no longer exists now that BayesWave support has been
        extracted into this standalone plugin.
        """
        return os.path.join(os.path.dirname(__file__), "configs", "bayeswave.ini")

    def build_dag(self, user=None, dryrun=False):
        """
        Construct a DAG file in order to submit a production to the
        condor scheduler using bayeswave_pipe.

        Parameters
        ----------
        production : str
           The production name.
        user : str
           The user accounting tag which should be used to run the job.
        dryrun : bool, optional
           If True, print the command without executing it.

        Raises
        ------
        PipelineException
           Raised if the construction of the DAG fails.
        """
        self._validate_components()

        if self.production.event.repository:
            try:
                gps_file = self.production.get_timefile()
            except AsimovFileNotFound:
                if "event time" in self.production.meta:
                    gps_time = self.production.get_meta("event time")
                    with set_directory(
                        os.path.join(
                            self.production.event.repository.directory, self.category
                        )
                    ):
                        with open("gpstime.txt", "w") as f:
                            f.write(str(gps_time))
                            gps_file = os.path.join(
                                f"{self.production.category}", "gpstime.txt"
                            )
                            self.production.event.repository.add_file(
                                "gpstime.txt", gps_file
                            )
                else:
                    raise PipelineException("Cannot find the event time.")
        else:
            gps_time = self.production.get_meta("event time")
            with open("gpstime.txt", "w") as f:
                f.write(str(gps_time))
                gps_file = os.path.join("gpstime.txt")

        if self.production.event.repository:
            # asimov.ini.RunConfiguration is now a bare wrapper around a
            # ConfigParser (just `.ini_loc` and `.ini`) -- it no longer has
            # `_get_user()`, `update_accounting()`, or `set_queue()` methods
            # (these were trimmed from core when the LALInference-specific
            # RunConfiguration API was cut down to the generic subset core
            # still needs). Calling them here always raised AttributeError,
            # so build_dag() previously crashed for essentially any
            # production whose event has a git repository -- i.e. real
            # usage. The accounting group and user are instead already
            # baked into the ini by the Liquid config_template at render
            # time (`accounting-group = {{ scheduler['accounting group']
            # }}` / `accounting-group-user = {{ config['condor']['user']
            # }}` in configs/bayeswave.ini), which runs in
            # Analysis.make_config() before build_dag() is ever called; no
            # further mutation is needed or, with the trimmed API,
            # possible. `user`/`queue` production.meta are accepted for
            # backwards compatibility but no longer have anywhere to go --
            # there is also no "queue" concept anywhere in bayeswave_pipe's
            # own ini schema.
            ini = self.production.get_configuration().ini_loc

        else:
            ini = f"{self.production.name}.ini"

        if self.production.rundir:
            rundir = self.production.rundir
        else:
            rundir = os.path.join(
                config.get("general", "rundir_default"),
                self.production.event.name,
                self.production.name,
            )
            self.production.rundir = rundir

        gps_time = self.production.get_meta("event time")

        # Resolve the bayeswave_pipe executable defensively rather than
        # assuming config["pipelines"]["environment"]/bin/bayeswave_pipe
        # exists: in minimal/containerised environments (e.g. the
        # htcondor/mini container used for e2e testing) that config value
        # may not point at the active environment. shutil.which() also
        # picks up an explicit per-production override via
        # production.meta["executable"], matching the approach adopted
        # upstream to allow bayeswave_pipe to run inside containers.
        default_executable = os.path.join(
            config.get("pipelines", "environment"), "bin", "bayeswave_pipe"
        )
        executable = self.production.meta.get("executable", default_executable)
        executable = shutil.which(executable) or shutil.which("bayeswave_pipe")
        if executable is None:
            raise PipelineException(
                "Cannot find the bayeswave_pipe executable",
                production=self.production.name,
            )

        command = [
            executable,
            f"--trigger-time={gps_time}",
        ]

        if "cache files" in self.production.meta["data"]:
            if len(self.production.meta["data"]["cache files"]) > 0:
                # Skip the datafind step if the data is already provided.
                command += ["--skip-datafind"]
                self.logger.info(
                    f"Using cache files: {self.production.meta['data']['cache files']}"
                )

        if "copy frames" in self.production.meta["scheduler"]:
            if self.production.meta["scheduler"]["copy frames"]:
                command += ["--copy-frames"]

        if "osg" in self.production.meta["scheduler"]:
            if self.production.meta["scheduler"]["osg"]:
                command += ["--transfer-files"]

                # --osg-deploy was renamed --igwn-pool upstream; the old
                # flag still works but bayeswave_pipe itself now reports it
                # as "OUTDATED. please use --igwn-pool instead."
                if "copy frames" not in self.production.meta["scheduler"]:
                    command += ["--igwn-pool"]
                if "copy frames" in self.production.meta["scheduler"]:
                    if not self.production.meta["scheduler"]["copy frames"]:
                        command += ["--igwn-pool"]

        command += [
            "-r",
            self.production.rundir,
            ini,
        ]

        self.logger.info(" ".join(command))
        if dryrun:
            print(" ".join(command))
            self.logger.info(" ".join(command))
        else:
            pipe = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            # stderr is merged into stdout above, so communicate()'s second
            # element is always empty -- everything is in `out`.
            out, _ = pipe.communicate()
            if "To submit:" not in str(out):
                self.production.status = "stuck"
                self.logger.error("Could not create a DAG file")
                self.logger.error(f"Command: {' '.join(command)}")
                self.logger.error(
                    "bayeswave_pipe output:\n"
                    f"{out.decode('utf-8', errors='replace') if isinstance(out, bytes) else out}"
                )
                raise PipelineException("The DAG file could not be created.")
            else:
                self.logger.info("DAG file created")
                self.logger.debug(out)

    def detect_completion(self):
        """
        Detect if the BayesWave job has completed.

        Returns
        -------
        bool
            True if the outputs required by this production's
            :attr:`run_mode` have been generated, False otherwise.

        Note
        ----
        For a PSD-only run (the default) this only checks for the PSDs,
        exactly as before. For a run which also requests a signal and/or
        glitch model (``"signal"``, ``"glitch"``, ``"full"`` or
        ``"coherence"`` mode), checking for the PSDs alone is not enough:
        the "clean" PSD-estimation phase finishes well before the
        signal/glitch post-processing does (they are independent models
        within the same BayesWave/BayesWavePost run), so that would report
        completion far too early. In that case this instead requires the
        reconstruction for every requested model and interferometer, plus
        the final megaplot summary page. For "coherence" mode this means
        both ``post/signal/`` and ``post/glitch/`` (BayesWave's own
        default model set writes each model to its own ``post/<model>/``
        directory, same as "signal"-only/"glitch"-only mode); for "full"
        mode it means the combined ``post/full/`` directory, which holds
        both a signal- and a glitch-prefixed reconstruction file. Either
        way, ``collect_assets()``'s reconstruction glob searches across
        every ``post/*/`` directory so this doesn't need to special-case
        which directory name is used.
        """
        mode = self.run_mode

        if mode == "psd":
            psds = self.collect_assets()["psds"]
            if len(list(psds.values())) > 0:
                return True
            else:
                self.logger.info("Bayeswave job completion was not detected.")
                return False

        required_components = []
        if mode in ("signal", "full", "coherence"):
            required_components.append("signal")
        if mode in ("glitch", "full", "coherence"):
            required_components.append("glitch")

        assets = self.collect_assets()
        reconstructions = assets["reconstructions"]
        ifos = self.production.meta["interferometers"]

        for component in required_components:
            available = reconstructions.get(component, {})
            if not all(ifo in available for ifo in ifos):
                self.logger.info("Bayeswave job completion was not detected.")
                return False

        # Require the final megaplot output too, so completion isn't
        # reported while megaplot.py is still generating the summary page.
        if not glob.glob(
            os.path.join(self.production.rundir, "trigtime*", "index.html")
        ):
            self.logger.info("Bayeswave job completion was not detected.")
            return False

        return True

    def _convert_psd(self, ascii_format, ifo):
        """
        Convert an ascii format PSD to XML.

        Parameters
        ----------
        ascii_format : str
           The location of the ascii format file.
        ifo : str
           The IFO which this PSD is for.

        Raises
        ------
        Exception
           If the PSD conversion fails.
        """
        command = [
            "convert_psd_ascii2xml",
            "--fname-psd-ascii",
            f"{ascii_format}",
            "--conventional-postfix",
            "--ifo",
            f"{ifo}",
        ]
        self.logger.info(" ".join(command))
        try:
            pipe = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
        except FileNotFoundError as e:
            # convert_psd_ascii2xml is not shipped by any current public
            # conda-forge package (checked bayeswave, bayeswaveutils,
            # lalinference and lalapps) -- it may only be available in
            # IGWN-internal environments. Fail clearly rather than letting
            # a bare FileNotFoundError propagate; the ascii-format PSD from
            # collect_assets() is still produced and stored regardless.
            raise PipelineException(
                "The convert_psd_ascii2xml executable could not be found; "
                "no XML-format PSD was produced (the ascii-format PSD is "
                "still available).",
                production=self.production.name,
            ) from e

        out, _ = pipe.communicate()

        # stderr is merged into stdout above (stderr=subprocess.STDOUT), so
        # communicate()'s second element is always empty. The previous
        # implementation branched on that always-empty value, so it could
        # never actually detect a failed conversion. Use the process's real
        # exit status instead.
        if pipe.returncode != 0:
            self.production.status = "stuck"
            raise PipelineException(
                f"An XML format PSD could not be created.\n{command}\n{out}",
                production=self.production.name,
            )
        else:
            asset = f"{ifo.upper()}-psd.xml.gz"
            git_location = os.path.join(
                self.production.category,
                "psds",
                f"{self.production.meta['likelihood']['sample rate']}",
            )
            self.production.event.repository.add_file(
                asset,
                os.path.join(git_location, f"{ifo}-psd.xml.gz"),
                commit_message=f"Added the xml format PSD for {ifo}.",
            )

    def after_completion(self):
        """
        Perform post-processing after the BayesWave job completes.

        This includes converting PSDs to XML format, collecting output
        pages, storing assets, and applying PSD suppressions if configured.
        """
        # convert_psd_ascii2xml ships with RIFT, not BayesWave, and this
        # plugin deliberately does not depend on RIFT -- so it may
        # legitimately not be installed alongside BayesWave. Check
        # availability upfront and skip cleanly when it's absent, rather
        # than always attempting the conversion and relying on the
        # try/except below to paper over what is then an expected, routine
        # condition rather than a real failure. The ascii-format PSDs are
        # unaffected either way.
        if shutil.which("convert_psd_ascii2xml"):
            try:
                for ifo, psd in self.collect_assets()["psds"].items():
                    self._convert_psd(ascii_format=psd, ifo=ifo)
            except Exception as e:
                self.logger.error("Failed to convert the PSDs to XML")
                self.logger.exception(e)
        else:
            self.logger.info(
                "convert_psd_ascii2xml is not available (it ships with "
                "RIFT, not BayesWave); skipping XML-format PSD conversion. "
                "The ascii-format PSDs are still produced and stored."
            )

        try:
            self.collect_pages()
        except (FileNotFoundError, IndexError) as e:
            # IndexError is raised by collect_pages() if no trigtime_*
            # directory exists yet (e.g. the megaplot DAG node hasn't
            # completed even though the PSD files detect_completion() looks
            # for already have); treat it the same as a missing file.
            self.logger.error("Failed to copy the megaplot output")
            self.logger.exception(e)

        try:
            self.collect_assets()
            self.store_assets()
        except Exception as e:
            self.logger.error("Failed to store the PSDs")
            self.logger.exception(e)

        try:
            self.store_reconstructions()
        except Exception as e:
            self.logger.error("Failed to store the reconstructions")
            self.logger.exception(e)

        if "supress" in self.production.meta.get("quality", {}):
            for ifo in self.production.meta["quality"]["supress"]:
                if ifo in self.production.meta["interferometers"]:
                    self.supress_psd(
                        ifo,
                        self.production.meta["quality"]["supress"][ifo]["lower"],
                        self.production.meta["quality"]["supress"][ifo]["upper"],
                    )

        self.production.meta.update(self.collect_assets())

        self.production.status = "uploaded"

    @property
    def flow(self):
        """
        Calculate the lower frequency for the bayeswave job.

        This is required as bayeswave needs to be passed the lowest
        minimum frequency from the list of interferometer
        lower frequencies.

        Returns
        -------
        float
            The minimum frequency across all interferometers.

        Raises
        ------
        ValueError
            If ``likelihood.minimum frequency`` isn't present as a
            non-empty per-interferometer dictionary in the production's
            metadata.

        Note
        ----
        This is computed fresh from ``production.meta`` on every access
        rather than cached, and deliberately not called during
        ``__init__``: pipeline construction happens inside
        ``Analysis.__init__``, before ``GravitationalWaveTransient``'s own
        ``quality`` -> ``likelihood`` migration for a deprecated
        ``quality.minimum frequency`` blueprint runs. An eager read here
        would see the pre-migration state and could raise even for a
        blueprint asimov itself will happily migrate and accept.
        """
        likelihood = self.production.meta.get("likelihood", {})
        min_freq = likelihood.get("minimum frequency")
        if not isinstance(min_freq, dict) or not min_freq:
            raise ValueError(
                "Minimum frequency must be specified in the 'likelihood' section. "
                "Please update your blueprint to include 'minimum frequency' in 'likelihood'."
            )
        return min(min_freq.values())

    @property
    def _components(self):
        """
        Resolve ``likelihood.components`` (and the pre-existing
        ``likelihood.coherence test`` term) into a normalised description
        of which BayesWave models this production should run.

        This mirrors the asimov v0.8-preview ledger vocabulary (see
        ``asimov/vocabulary.yaml``):

        .. code-block:: yaml

            likelihood:
              components:
                signal: wavelets      # none | wavelets | chirplets | cbc
                glitch: wavelets      # none | wavelets | chirplets
                noise:
                  psd: fit            # fit | fixed
                  lines: true
              coherence test: true

        Returns
        -------
        dict
            With keys ``signal``, ``glitch``, ``noise_psd``, ``lines``,
            ``chirplets`` and ``mode`` (one of ``"psd"``, ``"signal"``,
            ``"glitch"``, ``"full"`` or ``"coherence"``).

        Raises
        ------
        PipelineException
            If an unknown value is given for ``signal``, ``glitch`` or
            ``noise.psd``; if a combination BayesWave cannot (yet) run is
            requested (``signal: cbc`` or ``noise.psd: fixed``); or if
            ``coherence test: true`` is combined with ``signal: none`` or
            ``glitch: none`` (a coherence test needs both).

        Note
        ----
        Computed fresh from ``production.meta`` on every access, like
        :attr:`flow`, for the same construction-order reasons (see
        :attr:`flow`'s docstring) -- this must be safe to call from the
        ini template at render time, long after ``__init__``.
        """
        likelihood = self.production.meta.get("likelihood", {})
        components = likelihood.get("components")
        coherence_test = bool(likelihood.get("coherence test", False))

        if components is None and not coherence_test:
            # Nothing was requested: this MUST render/behave exactly as it
            # did before likelihood.components existed -- a PSD-only
            # cleanOnly run.
            return {
                "signal": "none",
                "glitch": "none",
                "noise_psd": "fit",
                "lines": True,
                "chirplets": False,
                "mode": "psd",
            }

        components = dict(components or {})
        # Unlike a coherence test (below), a bare components block does
        # NOT default signal/glitch to "wavelets" -- e.g.
        # components: {noise: {lines: false}} on its own must stay a
        # PSD-only run, not silently switch to a full signal+glitch run.
        signal = components.get("signal", "none")
        glitch = components.get("glitch", "none")
        noise = components.get("noise", {}) or {}
        noise_psd = noise.get("psd", "fit")
        lines = noise.get("lines", True)

        # coherence test implies a signal+glitch analysis (so their
        # evidences can be compared) unless the components block already
        # said otherwise explicitly.
        if coherence_test:
            if "signal" not in components:
                signal = "wavelets"
            if "glitch" not in components:
                glitch = "wavelets"

        if signal not in self._VALID_SIGNAL_COMPONENTS:
            raise PipelineException(
                f"Unknown value for likelihood.components.signal: {signal!r} "
                f"(expected one of {sorted(self._VALID_SIGNAL_COMPONENTS)})",
                production=self.production.name,
            )
        if glitch not in self._VALID_GLITCH_COMPONENTS:
            raise PipelineException(
                f"Unknown value for likelihood.components.glitch: {glitch!r} "
                f"(expected one of {sorted(self._VALID_GLITCH_COMPONENTS)})",
                production=self.production.name,
            )
        if noise_psd not in self._VALID_NOISE_PSD:
            raise PipelineException(
                f"Unknown value for likelihood.components.noise.psd: {noise_psd!r} "
                f"(expected one of {sorted(self._VALID_NOISE_PSD)})",
                production=self.production.name,
            )
        if signal == "cbc":
            raise PipelineException(
                "likelihood.components.signal: cbc is not yet supported by "
                "the BayesWave plugin.",
                production=self.production.name,
            )
        if noise_psd == "fixed":
            raise PipelineException(
                "likelihood.components.noise.psd: fixed is not yet "
                "supported by the BayesWave plugin.",
                production=self.production.name,
            )

        signal_on = signal in ("wavelets", "chirplets")
        glitch_on = glitch in ("wavelets", "chirplets")

        if coherence_test and not (signal_on and glitch_on):
            raise PipelineException(
                "likelihood.coherence test: true requires both a signal "
                "and a glitch model (likelihood.components.signal and "
                ".glitch must not be 'none').",
                production=self.production.name,
            )

        # BayesWave's --chirplets flag is a single global toggle (it adds
        # the chirplet basis to whichever models are enabled) rather than
        # an independent per-model choice, so "chirplets" for either
        # component turns it on for the run as a whole.
        chirplets = signal == "chirplets" or glitch == "chirplets"

        if coherence_test:
            # A coherence test needs signal, glitch AND noise run as
            # genuinely separate RJMCMC phases with real, independent
            # evidences -- that's BayesWave's own default model set (no
            # *Only restriction flag at all), not --fullOnly, which runs
            # only the *combined* signal+glitch model and leaves
            # signal/glitch/noise as unrun placeholders in evidence.dat
            # (see _MODE_FLAG's docstring comment above).
            mode = "coherence"
        elif signal_on and glitch_on:
            mode = "full"
        elif signal_on:
            mode = "signal"
        elif glitch_on:
            mode = "glitch"
        else:
            mode = "psd"

        return {
            "signal": signal,
            "glitch": glitch,
            "noise_psd": noise_psd,
            "lines": bool(lines),
            "chirplets": chirplets,
            "mode": mode,
        }

    def _validate_components(self):
        """
        Validate ``likelihood.components`` for this production.

        Called from :meth:`build_dag` so that an unsupported or unknown
        combination is rejected with a clear ``PipelineException`` before
        a DAG is built, rather than failing obscurely inside BayesWave
        itself (or not at all).
        """
        self._components

    @property
    def run_mode(self):
        """
        The BayesWave run mode implied by ``likelihood.components``.

        Returns
        -------
        str
            One of ``"psd"`` (today's default: on-source PSD estimation
            only), ``"signal"``, ``"glitch"``, ``"full"`` (the *combined*
            signal+glitch model, no separate evidences) or ``"coherence"``
            (BayesWave's own default model set: signal, glitch and noise
            each run as genuinely separate phases, so their evidences can
            be compared -- what ``likelihood.coherence test: true`` needs).
        """
        return self._components["mode"]

    @property
    def model_flags(self):
        """
        The BayesWave model-selection flags to set (as bare, value-less
        keys) in ``[bayeswave_options]`` for this production's
        :attr:`run_mode`.

        Returns
        -------
        list of str
            E.g. ``["cleanOnly"]`` or ``["fullOnly", "chirplets"]``, or
            ``[]`` (or ``["chirplets"]``) for :attr:`run_mode`
            ``"coherence"``, which deliberately sets no model-restriction
            flag at all so BayesWave's own defaults apply.
        """
        flags = []
        mode_flag = self._MODE_FLAG[self.run_mode]
        if mode_flag is not None:
            flags.append(mode_flag)
        if self._components["chirplets"]:
            flags.append("chirplets")
        return flags

    @property
    def bayesline_enabled(self):
        """
        Whether BayesLine spectral-line modelling (``--bayesLine``) should
        be enabled, from ``likelihood.components.noise.lines`` (default
        ``True``, matching today's unconditional default).
        """
        return self._components["lines"]

    def before_submit(self):
        """
        Modify submission files before submitting the DAG.

        This method adds `request_disk` directives to submission files
        and fixes Python shebangs to use the correct environment.
        """
        sub_files = glob.glob(f"{self.production.rundir}/*.sub")
        for sub_file in sub_files:
            with open(sub_file, "r") as f_handle:
                original = f_handle.read()
            with open(sub_file, "w") as f_handle:
                self.logger.info(f"Adding request_disk = {64000} to {sub_file}")
                f_handle.write(f"request_disk = {64000}\n" + original)
        python_files = glob.glob(f"{self.production.rundir}/*.py")
        for py_file in python_files:
            with open(py_file, "r") as f_handle:
                original = f_handle.read()
            with open(py_file, "w") as f_handle:
                self.logger.info("Fixing shebang")
                path = os.path.join(
                    config.get("pipelines", "environment"), "bin", "python"
                )
                f_handle.write(f"#! {path}\n" + original)

    def submit_dag(self, dryrun=False):
        """
        Submit a DAG file to the scheduler.

        Parameters
        ----------
        dryrun: bool
           If True then the DAG will not be submitted but all of the
           commands will be printed to stdout.

        Returns
        -------
        int
           The cluster ID assigned to the running DAG file.

        Raises
        ------
        PipelineException
           This will be raised if the pipeline fails to submit the job.
        """
        self.before_submit()

        # bayeswave_pipe names the generated top-level DAG file after the
        # basename of the --workdir it was given (see
        # `dagname = os.path.join(workdir, os.path.basename(workdir))` in
        # bayeswave_pipe itself), not after the production name directly.
        # These are the same string under asimov's own default rundir
        # convention (rundir = .../<event>/<production.name>), but computing
        # it from the actual rundir is correct even if that convention is
        # overridden.
        dag_filename = f"{os.path.basename(self.production.rundir)}.dag"
        batch_name = f"bwave/{self.production.event.name}/{self.production.name}"

        self.logger.info(
            f"Submitting DAG: {dag_filename} with batch name: {batch_name}"
        )

        if dryrun:
            print(f"Would submit DAG: {dag_filename} with batch name: {batch_name}")

        else:
            with set_directory(self.production.rundir):
                try:
                    # Use asimov's scheduler abstraction (HTCondor or Slurm)
                    # rather than shelling out to condor_submit_dag directly.
                    cluster_id = self.scheduler.submit_dag(
                        dag_file=dag_filename,
                        batch_name=batch_name,
                    )

                    self.production.status = "running"
                    self.production.job_id = int(cluster_id)
                    self.logger.info(
                        f"Successfully submitted to cluster {self.production.job_id}"
                    )
                    return (int(cluster_id),)

                except FileNotFoundError as e:
                    self.logger.exception(e)
                    raise PipelineException(
                        "It looks like the scheduler isn't properly configured.\n"
                        f"Failed to submit DAG file: {dag_filename}"
                    ) from e
                except RuntimeError as e:
                    self.logger.exception(e)
                    raise PipelineException(
                        f"The DAG file could not be submitted: {e}",
                    ) from e

    def upload_assets(self):
        """
        Upload the PSDs from this job to the event repository.
        """
        sample = self.production.meta["likelihood"]["sample rate"]
        git_location = os.path.join(self.category, "psds")

        for detector, asset in self.collect_assets()["psds"].items():
            self.production.event.repository.add_file(
                asset,
                os.path.join(git_location, str(sample), f"{detector}-psd.dat"),
                commit_message=f"Added the PSD for {detector}.",
            )

    def store_assets(self):
        """
        Add the assets to the Asimov store.

        This stores PSDs in the central Asimov storage location
        for use by other productions.
        """
        sample_rate = self.production.meta["likelihood"]["sample rate"]
        self.logger.info(self.collect_assets())
        for detector, asset in self.collect_assets()["psds"].items():
            store = Store(root=config.get("storage", "directory"))
            try:
                store.add_file(
                    self.production.event.name,
                    self.production.name,
                    file=asset,
                    new_name=f"{detector}-{sample_rate}-psd.dat",
                )
            except Exception as e:
                self.logger.error(
                    f"There was a problem committing the PSD for {detector} to the store."
                )
                self.logger.exception(e)

    def store_reconstructions(self):
        """
        Add signal/glitch/full waveform reconstructions to the event
        repository and the Asimov store.

        These are the small, text-format median reconstruction files
        BayesWavePost produces (see :meth:`collect_assets`); they are
        stored the same way as the PSDs, guarded per-file the same way, so
        one failure doesn't stop the others.
        """
        sample_rate = self.production.meta["likelihood"]["sample rate"]
        reconstructions = self.collect_assets()["reconstructions"]
        for component, per_ifo in reconstructions.items():
            for detector, asset in per_ifo.items():
                new_name = f"{detector}-{component}-{sample_rate}-reconstruction.dat"

                try:
                    self.production.event.repository.add_file(
                        asset,
                        os.path.join(
                            self.production.category,
                            "reconstructions",
                            component,
                            f"{detector}-reconstruction.dat",
                        ),
                        commit_message=(
                            f"Added the {component} reconstruction for {detector}."
                        ),
                    )
                except Exception as e:
                    self.logger.error(
                        f"There was a problem committing the {component} "
                        f"reconstruction for {detector} to the repository."
                    )
                    self.logger.exception(e)

                try:
                    store = Store(root=config.get("storage", "directory"))
                    store.add_file(
                        self.production.event.name,
                        self.production.name,
                        file=asset,
                        new_name=new_name,
                    )
                except Exception as e:
                    self.logger.error(
                        f"There was a problem committing the {component} "
                        f"reconstruction for {detector} to the store."
                    )
                    self.logger.exception(e)

    def collect_logs(self):
        """
        Collect all of the log files which have been produced by this production.

        Returns
        -------
        dict
            Dictionary mapping log file names to their contents.
        """
        messages = {}

        logfile = os.path.join(
            config.get("logging", "location"),
            self.production.event.name,
            self.production.name,
            "asimov.log",
        )
        with open(logfile, "r") as log_f:
            message = log_f.read()
            messages["production"] = message

        logs = glob.glob(f"{self.production.rundir}/logs/*.err") + glob.glob(
            f"{self.production.rundir}/*.err"
        )
        for log in logs:
            with open(log, "r") as log_f:
                message = log_f.read()
                messages[log.split("/")[-1]] = message
        return messages

    def collect_assets(self):
        """
        Collect the assets for this job and commit them to the event repository.

        Since this job also generates the PSDs these should be added to the
        production ledger.

        Returns
        -------
        dict
            Dictionary containing 'psds' and 'xml psds' keys with paths to
            the generated power spectral density files.
        """
        psds = {}
        for det in self.production.meta["interferometers"]:
            # NOTE: this glob legitimately returns [] on every poll before
            # the run completes -- that's the whole point of
            # detect_completion() calling this repeatedly. A previous
            # version of this loop left `asset` as that empty list and then
            # called os.path.exists(asset) unconditionally, which raises
            # TypeError (os.path.exists only accepts a path, not a list) --
            # i.e. it crashed on every single monitoring poll prior to
            # completion, not just at the end. Existing unit tests never
            # caught this because they mock os.path.exists to always return
            # True. Guard explicitly instead.
            matches = glob.glob(
                os.path.join(
                    self.production.rundir,
                    "trigtime*",
                    "post",
                    "clean",
                    f"glitch_median_PSD_forLI_{det}.dat",
                )
            )
            if matches and os.path.exists(matches[0]):
                psds[det] = matches[0]

        outputs = {}
        outputs["psds"] = psds

        xml_psds = {}
        for det in self.production.meta["interferometers"]:
            asset = os.path.join(
                self.production.event.repository.directory,
                self.production.category,
                "psds",
                f"{self.production.meta['likelihood']['sample rate']}",
                f"{det.upper()}-psd.xml.gz",
            )
            if os.path.exists(asset):
                xml_psds[det] = os.path.abspath(asset)

        outputs["xml psds"] = xml_psds

        # Reconstructions: BayesWavePost writes
        # post/<model name>/<model type>_median_time_domain_waveform_<ifo>.dat
        # for each model it post-processes (verified against
        # BayesWavePost.c post_process_model()). For a signal-only or
        # glitch-only run <model name> and <model type> match ("signal" or
        # "glitch"); for a full (signal+glitch) run <model name> is always
        # "full" but files for both "signal" and "glitch" (and "full",
        # the combined waveform) are written inside post/full/. Search by
        # <model type> prefix across every post/* directory so all three
        # cases are found without needing to know run_mode here.
        reconstructions = {}
        for det in self.production.meta["interferometers"]:
            for component in ("signal", "glitch", "full"):
                matches = glob.glob(
                    os.path.join(
                        self.production.rundir,
                        "trigtime*",
                        "post",
                        "*",
                        f"{component}_median_time_domain_waveform_{det}.dat",
                    )
                )
                if matches and os.path.exists(matches[0]):
                    reconstructions.setdefault(component, {})[det] = matches[0]

        outputs["reconstructions"] = reconstructions

        # Bayes factors: BayesWave.c's fprintf()s of the "signal"/"glitch"/
        # "noise" lines to trigtime_*/evidence.dat are UNCONDITIONAL --
        # they run regardless of whether that model's RJMCMC phase actually
        # executed (verified in BayesWave.c: each fprintf() sits *outside*
        # its "if(data->xModelFlag) {...}" block), so a model that wasn't
        # requested still gets a placeholder "<model> 0 0" line, not an
        # omitted one. Only "coherence" mode (BayesWave's own default model
        # set: no restriction flag) actually runs signal, glitch AND noise
        # each as genuinely separate phases with real, comparable
        # evidences -- every other mode (including "full", whose combined
        # run only ever produces a real "full" evidence) would produce
        # misleading zero-based Bayes factors if parsed the same way, so
        # only compute/report them for "coherence" mode.
        bayes_factors = {}
        if self.run_mode == "coherence":
            evidence_matches = glob.glob(
                os.path.join(self.production.rundir, "trigtime*", "evidence.dat")
            )
            if evidence_matches and os.path.exists(evidence_matches[0]):
                bayes_factors = self._parse_bayes_factors(evidence_matches[0])

        outputs["bayes factors"] = bayes_factors

        # Skymap: megaplot.py only produces plots/skymap.png once a signal
        # model has been post-processed and sky-location samples exist
        # (verified against megaplot.py's skymap_posterior(), only called
        # when mod == 'signal').
        skymap_matches = glob.glob(
            os.path.join(self.production.rundir, "trigtime*", "plots", "skymap.png")
        )
        if skymap_matches and os.path.exists(skymap_matches[0]):
            outputs["skymap"] = os.path.abspath(skymap_matches[0])

        return outputs

    @staticmethod
    def _parse_bayes_factors(evidence_file):
        """
        Parse a BayesWave ``evidence.dat`` file into log Bayes factors.

        Only meaningful for a "coherence" mode run (see
        :attr:`collect_assets`'s docstring comment) -- BayesWave always
        writes a ``"<model> <logZ> <var>"`` line for signal/glitch/noise
        regardless of whether that model actually ran, so this function
        trusts its caller to only invoke it when all of the models it will
        compare genuinely did.

        Parameters
        ----------
        evidence_file : str
            Path to a ``trigtime_*/evidence.dat`` file, one
            ``"<model> <logZ> <var>"`` line per model (e.g.
            ``"signal 12.3 0.1"``, verified against BayesWave.c).

        Returns
        -------
        dict
            Mapping of e.g. ``"signal:noise"`` to the log Bayes factor
            ``logZ[signal] - logZ[noise]``, for every pair of models whose
            log evidences are both present. Empty if fewer than two
            recognised models are present.
        """
        log_z = {}
        with open(evidence_file, "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                model = parts[0].lower()
                try:
                    log_z[model] = float(parts[1])
                except ValueError:
                    continue

        bayes_factors = {}
        pairs = [("signal", "noise"), ("signal", "glitch"), ("glitch", "noise")]
        for numerator, denominator in pairs:
            if numerator in log_z and denominator in log_z:
                bayes_factors[f"{numerator}:{denominator}"] = (
                    log_z[numerator] - log_z[denominator]
                )

        return bayes_factors

    def supress_psd(self, ifo, fmin, fmax):
        """
        Suppress portions of a PSD.

        Author: Carl-Johan Haster - August 2020
        (Updated for asimov by Daniel Williams - November 2020)

        Parameters
        ----------
        ifo : str
            Interferometer name (e.g., 'H1', 'L1', 'V1')
        fmin : float
            Lower frequency bound for suppression region in Hz
        fmax : float
            Upper frequency bound for suppression region in Hz
        """
        store = Store(root=config.get("storage", "directory"))
        sample_rate = self.production.meta["likelihood"]["sample rate"]
        orig_PSD_file = np.genfromtxt(
            os.path.join(
                self.production.event.repository.directory,
                self.category,
                "psds",
                str(sample_rate),
                f"{ifo}-psd.dat",
            )
        )

        self.logger.info("PSD supression has been set")
        self.logger.info(
            f"{ifo}-psd.dat will be supressed between {fmin}-Hz and {fmax}-Hz"
        )

        freq = orig_PSD_file[:, 0]
        PSD = orig_PSD_file[:, 1]

        suppression_region = np.logical_and(
            np.greater_equal(freq, fmin), np.less_equal(freq, fmax)
        )

        # Suppress the PSD in this region
        PSD[suppression_region] = 1.0

        new_PSD = np.vstack([freq, PSD]).T

        asset = f"{ifo}-psd.dat"
        np.savetxt(asset, new_PSD, fmt="%+.5e")

        destination = os.path.join(
            self.category, "psds", str(sample_rate), f"{ifo}-psd.dat"
        )

        self.logger.info(
            f"{ifo}-psd.dat has been supressed between {fmin}-Hz and {fmax}-Hz"
        )

        try:
            self.production.event.repository.add_file(asset, destination)
        except Exception as e:
            self.logger.error(
                "The supressed PSD could not be committed to the repository"
            )
            self.logger.exception(e)

        copyfile(asset, f"{ifo}-{sample_rate}-psd-suppresed.dat")
        try:
            store.add_file(
                self.production.event.name,
                self.production.name,
                file=f"{ifo}-{sample_rate}-psd-suppresed.dat",
            )
        except AlreadyPresentException:
            self.logger.warning(
                "Attempted to add a supressed PSD which already exists."
            )

    def resurrect(self):
        """
        Attempt to resurrect a failed job.

        This method will resubmit the DAG using rescue files if fewer
        than 5 rescue attempts have been made.
        """
        count = len(glob.glob(os.path.join(self.production.rundir, "*.dag.rescue*")))

        if (count < 5) and (count > 0):
            self.submit_dag()
            self.logger.info(f"Bayeswave job was resurrected for the {count} time.")
        else:
            self.logger.error(
                "Bayeswave resurrection not completed as there have already been 5 attempts"
            )

    def html(self):
        """
        Return the HTML representation of this pipeline.

        Returns
        -------
        str
            HTML string for displaying pipeline results in web interfaces.
        """
        pages_dir = os.path.join(self.production.event.name, self.production.name)
        out = ""
        if self.production.status in {"finished", "uploaded"}:
            out += """<div class="asimov-pipeline">"""
            out += (
                f"""<p><a href="{pages_dir}/index.html">Full Megaplot output</a></p>"""
            )
            out += f"""<img height=200 src="{pages_dir}/plots/clean_whitened_residual_histograms.png"</src>"""

            # Reconstruction plots (only present for a signal and/or glitch
            # run -- see collect_assets()/after_completion(), which store
            # 'reconstructions' into production.meta on completion). Uses
            # the plot filenames megaplot.py writes into plots/ (verified
            # against megaplot.py's plot_waveform()):
            # "<model>_waveform_<ifo>.png".
            reconstructions = self.production.meta.get("reconstructions", {})
            for component in ("signal", "glitch"):
                for ifo in reconstructions.get(component, {}):
                    out += (
                        f"""<img height=200 """
                        f"""src="{pages_dir}/plots/{component}_waveform_{ifo}.png">"""
                    )

            # Bayes factor table (only present when evidence.dat could be
            # parsed -- see _parse_bayes_factors()).
            bayes_factors = self.production.meta.get("bayes factors", {})
            if bayes_factors:
                out += """<table class="asimov-bayeswave-bayes-factors">"""
                out += "<tr><th>Model comparison</th><th>log Bayes factor</th></tr>"
                for comparison, value in bayes_factors.items():
                    out += f"<tr><td>{comparison}</td><td>{value:.2f}</td></tr>"
                out += "</table>"

            # Skymap (only produced by megaplot.py for a signal run -- see
            # collect_assets()).
            if self.production.meta.get("skymap"):
                out += f"""<img height=200 src="{pages_dir}/plots/skymap.png">"""

            out += """</div>"""

        return out

    def collect_pages(self):
        """
        Collect the HTML output of the pipeline.

        This copies the megaplot output to the web directory for
        visualization.
        """
        results_dir = glob.glob(f"{self.production.rundir}/trigtime_*")[0]
        pages_dir = os.path.join(
            config.get("general", "webroot"),
            self.production.event.name,
            self.production.name,
        )
        os.makedirs(pages_dir, exist_ok=True)
        copyfile(
            os.path.join(results_dir, "index.html"),
            os.path.join(pages_dir, "index.html"),
        )
        copytree(
            os.path.join(results_dir, "html"),
            os.path.join(pages_dir, "html"),
            dirs_exist_ok=True,
        )
        copytree(
            os.path.join(results_dir, "plots"),
            os.path.join(pages_dir, "plots"),
            dirs_exist_ok=True,
        )
