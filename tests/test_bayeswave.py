"""Tests for the BayesWave pipeline integration."""

import os
from unittest.mock import MagicMock, Mock, mock_open, patch

import numpy as np
import pytest
from asimov.pipeline import PipelineException
from liquid import Liquid

from asimov_bayeswave import BayesWave

CONFIG_TEMPLATE = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__), "..", "asimov_bayeswave", "configs", "bayeswave.ini"
    )
)


class TestBayesWaveInit:
    """Test BayesWave initialization."""

    def test_init_success(self, mock_production, mock_config):
        """Test successful initialization."""
        pipeline = BayesWave(mock_production)
        assert pipeline.name == "BayesWave"
        assert pipeline.production == mock_production
        assert "wait" in pipeline.STATUS

    def test_init_wrong_pipeline(self, mock_production, mock_config):
        """Test initialization with wrong pipeline name."""
        mock_production.pipeline = "bilby"
        with pytest.raises(PipelineException, match="Pipeline mismatch"):
            BayesWave(mock_production)

    def test_init_does_not_eagerly_evaluate_flow(self, mock_production, mock_config):
        """Regression test for a real construction-order bug (matches the
        fix landed upstream in asimov core before this pipeline was
        extracted into a standalone plugin, see commit a05530e6 / PR #130
        there): pipeline construction happens inside Analysis.__init__,
        *before* GravitationalWaveTransient's own quality->likelihood
        migration for a deprecated 'quality.minimum frequency' blueprint
        runs. A previous version of __init__ eagerly evaluated self.flow
        (which requires 'likelihood.minimum frequency') to pre-cache it
        into production.meta['quality']['lowest minimum frequency'] for the
        ini template -- so constructing a BayesWave analysis from a
        quality-only blueprint always crashed with a ValueError, before the
        migration ever got a chance to run. Simulate that pre-migration
        state directly (only 'quality.minimum frequency' set, nothing under
        'likelihood') and confirm construction no longer touches flow at
        all."""
        mock_production.meta["likelihood"].pop("minimum frequency", None)
        mock_production.meta["quality"] = {"minimum frequency": {"H1": 20}}

        # Should not raise, unlike the pre-fix behaviour.
        pipeline = BayesWave(mock_production)
        assert pipeline.production is mock_production

        # And confirm the eager cache this bug came from is genuinely gone,
        # not just made non-crashing:
        assert "lowest minimum frequency" not in mock_production.meta["quality"]


class TestBayesWaveFlow:
    """Test the flow property."""

    def test_flow_calculation(self, mock_production, mock_config):
        """Test minimum frequency calculation."""
        mock_production.meta["likelihood"]["minimum frequency"] = {
            "H1": 20,
            "L1": 25,
            "V1": 15,
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.flow == 15

    def test_flow_single_ifo(self, mock_production, mock_config):
        """Test flow with single interferometer."""
        mock_production.meta["likelihood"]["minimum frequency"] = {"H1": 30}
        pipeline = BayesWave(mock_production)
        assert pipeline.flow == 30

    def test_flow_computed_fresh_not_cached(self, mock_production, mock_config):
        """flow must be computed fresh from production.meta on every access
        (not cached at construction time) -- this is the property that
        makes the construction-order fix above safe: the ini template can
        call pipeline.flow at render time, long after __init__, and it will
        see whatever migration/updates have happened to production.meta by
        then."""
        pipeline = BayesWave(mock_production)
        mock_production.meta["likelihood"]["minimum frequency"] = {"H1": 42}
        assert pipeline.flow == 42

    def test_flow_raises_when_minimum_frequency_missing(
        self, mock_production, mock_config
    ):
        """A clear, specific error rather than an AttributeError from
        calling .values() on a non-dict default."""
        mock_production.meta["likelihood"].pop("minimum frequency", None)
        pipeline = BayesWave(mock_production)
        with pytest.raises(ValueError, match="likelihood"):
            pipeline.flow

    def test_flow_raises_when_minimum_frequency_empty(
        self, mock_production, mock_config
    ):
        mock_production.meta["likelihood"]["minimum frequency"] = {}
        pipeline = BayesWave(mock_production)
        with pytest.raises(ValueError, match="likelihood"):
            pipeline.flow


class TestConfigTemplate:
    """Test the config_template property used by asimov's `manage build`
    to render an ini when one doesn't already exist in the event
    repository."""

    def test_config_template_is_a_real_bundled_file(self, mock_production, mock_config):
        pipeline = BayesWave(mock_production)
        assert os.path.exists(pipeline.config_template)

    def test_config_template_is_named_bayeswave_ini(self, mock_production, mock_config):
        pipeline = BayesWave(mock_production)
        assert os.path.basename(pipeline.config_template) == "bayeswave.ini"


class TestBuildDag:
    """Test DAG building."""

    @patch("asimov_bayeswave.bayeswave.shutil.which")
    @patch("asimov_bayeswave.bayeswave.subprocess.Popen")
    @patch("asimov_bayeswave.bayeswave.open", new_callable=mock_open)
    def test_build_dag_success(
        self, mock_file, mock_popen, mock_which, mock_production, mock_config
    ):
        """Test successful DAG building."""
        mock_which.return_value = "/opt/conda/bin/bayeswave_pipe"

        # Mock successful bayeswave_pipe execution
        mock_process = Mock()
        mock_process.communicate.return_value = (b"To submit: condor_submit", b"")
        mock_popen.return_value = mock_process

        # Mock get_configuration
        mock_ini = MagicMock()
        mock_ini._get_user = Mock(return_value="test.user")
        mock_ini.ini_loc = "/tmp/test.ini"
        mock_production.get_configuration.return_value = mock_ini

        pipeline = BayesWave(mock_production)
        pipeline.build_dag(user="test.user", dryrun=False)

        # Verify bayeswave_pipe was called
        assert mock_popen.called
        call_args = mock_popen.call_args[0][0]
        assert "bayeswave_pipe" in call_args[0]
        assert any("--trigger-time" in arg for arg in call_args)

    @patch("asimov_bayeswave.bayeswave.shutil.which")
    @patch("asimov_bayeswave.bayeswave.subprocess.Popen")
    def test_build_dag_failure(
        self, mock_popen, mock_which, mock_production, mock_config
    ):
        """Test DAG building failure."""
        mock_which.return_value = "/opt/conda/bin/bayeswave_pipe"

        # Mock failed bayeswave_pipe execution
        mock_process = Mock()
        mock_process.communicate.return_value = (b"Error occurred", b"stderr")
        mock_popen.return_value = mock_process

        mock_ini = MagicMock()
        mock_ini._get_user = Mock(return_value="test.user")
        mock_ini.ini_loc = "/tmp/test.ini"
        mock_production.get_configuration.return_value = mock_ini

        pipeline = BayesWave(mock_production)

        with pytest.raises(PipelineException, match="DAG file could not be created"):
            pipeline.build_dag(user="test.user", dryrun=False)

    @patch("asimov_bayeswave.bayeswave.shutil.which")
    def test_build_dag_dryrun(self, mock_which, mock_production, mock_config, capsys):
        """Test DAG building in dryrun mode."""
        mock_which.return_value = "/opt/conda/bin/bayeswave_pipe"

        mock_ini = MagicMock()
        mock_ini._get_user = Mock(return_value="test.user")
        mock_ini.ini_loc = "/tmp/test.ini"
        mock_production.get_configuration.return_value = mock_ini

        pipeline = BayesWave(mock_production)
        pipeline.build_dag(user="test.user", dryrun=True)

        captured = capsys.readouterr()
        assert "bayeswave_pipe" in captured.out

    @patch("asimov_bayeswave.bayeswave.shutil.which", return_value=None)
    def test_build_dag_missing_executable_raises_clear_exception(
        self, mock_which, mock_production, mock_config
    ):
        """Regression test: build_dag() must not silently proceed with a
        broken/empty executable path (which would only surface much later
        as a confusing HTCondor "No such file or directory" hold) -- it
        should fail immediately and clearly if bayeswave_pipe can't be
        found anywhere."""
        mock_ini = MagicMock()
        mock_ini._get_user = Mock(return_value="test.user")
        mock_ini.ini_loc = "/tmp/test.ini"
        mock_production.get_configuration.return_value = mock_ini

        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="bayeswave_pipe"):
            pipeline.build_dag(user="test.user", dryrun=False)


class TestSubmitDag:
    """Test DAG submission."""

    # submit_dag() uses the asimov >=0.7 scheduler abstraction
    # (self.scheduler.submit_dag(...), from the base Pipeline class) rather
    # than hand-rolling a `condor_submit_dag` subprocess call -- matching
    # the pattern asimov core's own final pre-extraction revision of this
    # pipeline used, and the same migration already made in the sibling
    # asimov-lalinference plugin. This gets Slurm support for free and
    # avoids parsing subprocess stdout for "submitted to cluster ([\\d]+)".

    @patch("asimov_bayeswave.bayeswave.glob.glob")
    @patch("asimov_bayeswave.bayeswave.set_directory")
    def test_submit_dag_success(
        self, mock_set_dir, mock_glob, mock_production, mock_config
    ):
        """Test successful DAG submission."""
        mock_set_dir.return_value.__enter__ = Mock()
        mock_set_dir.return_value.__exit__ = Mock(return_value=False)
        mock_glob.return_value = []  # no .sub/.py files for before_submit()

        pipeline = BayesWave(mock_production)
        pipeline._scheduler = Mock()
        pipeline._scheduler.submit_dag.return_value = 12345

        result = pipeline.submit_dag(dryrun=False)

        assert result == (12345,)
        assert mock_production.job_id == 12345
        assert mock_production.status == "running"

    @patch("asimov_bayeswave.bayeswave.glob.glob")
    @patch("asimov_bayeswave.bayeswave.set_directory")
    def test_submit_dag_uses_scheduler_abstraction(
        self, mock_set_dir, mock_glob, mock_production, mock_config
    ):
        """The scheduler is asked to submit a DAG named after the rundir's
        basename (matching what bayeswave_pipe itself names the generated
        top-level DAG file -- see dagname = os.path.join(workdir,
        os.path.basename(workdir)) in bayeswave_pipe), not hand-rolled
        subprocess/condor_submit_dag."""
        mock_set_dir.return_value.__enter__ = Mock()
        mock_set_dir.return_value.__exit__ = Mock(return_value=False)
        mock_glob.return_value = []

        pipeline = BayesWave(mock_production)
        pipeline._scheduler = Mock()
        pipeline._scheduler.submit_dag.return_value = 12345

        pipeline.submit_dag(dryrun=False)

        kwargs = pipeline._scheduler.submit_dag.call_args.kwargs
        assert kwargs["dag_file"] == f"{os.path.basename(mock_production.rundir)}.dag"
        assert mock_production.event.name in kwargs["batch_name"]
        assert mock_production.name in kwargs["batch_name"]

    @patch("asimov_bayeswave.bayeswave.glob.glob")
    @patch("asimov_bayeswave.bayeswave.set_directory")
    def test_submit_dag_dryrun_does_not_call_scheduler(
        self, mock_set_dir, mock_glob, mock_production, mock_config
    ):
        mock_glob.return_value = []
        pipeline = BayesWave(mock_production)
        pipeline._scheduler = Mock()

        pipeline.submit_dag(dryrun=True)

        pipeline._scheduler.submit_dag.assert_not_called()

    @patch("asimov_bayeswave.bayeswave.glob.glob")
    @patch("asimov_bayeswave.bayeswave.set_directory")
    def test_submit_dag_failure(
        self, mock_set_dir, mock_glob, mock_production, mock_config
    ):
        """Test DAG submission failure."""
        mock_set_dir.return_value.__enter__ = Mock()
        mock_set_dir.return_value.__exit__ = Mock(return_value=False)
        mock_glob.return_value = []

        pipeline = BayesWave(mock_production)
        pipeline._scheduler = Mock()
        pipeline._scheduler.submit_dag.side_effect = RuntimeError("could not submit")

        with pytest.raises(PipelineException, match="DAG file could not be submitted"):
            pipeline.submit_dag(dryrun=False)

    @patch("asimov_bayeswave.bayeswave.glob.glob")
    @patch("asimov_bayeswave.bayeswave.set_directory")
    def test_submit_dag_scheduler_not_configured(
        self, mock_set_dir, mock_glob, mock_production, mock_config
    ):
        mock_set_dir.return_value.__enter__ = Mock()
        mock_set_dir.return_value.__exit__ = Mock(return_value=False)
        mock_glob.return_value = []

        pipeline = BayesWave(mock_production)
        pipeline._scheduler = Mock()
        pipeline._scheduler.submit_dag.side_effect = FileNotFoundError("no dag")

        with pytest.raises(PipelineException, match="scheduler"):
            pipeline.submit_dag(dryrun=False)


class TestBeforeSubmit:
    """Test pre-submission modifications."""

    @patch("builtins.open", new_callable=mock_open, read_data="original content")
    @patch("asimov_bayeswave.bayeswave.glob.glob")
    def test_before_submit_adds_disk_request(
        self, mock_glob, mock_file, mock_production, mock_config
    ):
        """Test that request_disk is added to submission files."""
        mock_glob.side_effect = [
            ["/tmp/test.sub"],  # First call for .sub files
            [],  # Second call for .py files
        ]

        pipeline = BayesWave(mock_production)
        pipeline.before_submit()

        # Check that file was opened for reading and writing
        assert mock_file.call_count >= 2

    @patch("builtins.open", new_callable=mock_open, read_data="original content")
    @patch("asimov_bayeswave.bayeswave.glob.glob")
    def test_before_submit_fixes_shebang(
        self, mock_glob, mock_file, mock_production, mock_config
    ):
        """Test that Python shebang is fixed."""
        mock_glob.side_effect = [
            [],  # First call for .sub files
            ["/tmp/test.py"],  # Second call for .py files
        ]

        pipeline = BayesWave(mock_production)
        pipeline.before_submit()

        # Verify file operations occurred
        assert mock_file.call_count >= 2


class TestCollectAssets:
    """Test asset collection."""

    @patch("asimov_bayeswave.bayeswave.os.path.exists")
    @patch("asimov_bayeswave.bayeswave.glob.glob")
    def test_collect_assets_psds(
        self, mock_glob, mock_exists, mock_production, mock_config
    ):
        """Test PSD collection."""

        # Only the PSD glob pattern should match; every other pattern this
        # collects (reconstructions, evidence.dat, skymap) legitimately
        # matches nothing for a PSD-only run.
        def glob_side_effect(pattern):
            if "glitch_median_PSD_forLI_H1.dat" in pattern:
                return [
                    "/tmp/test_rundir/trigtime_123/post/clean/"
                    "glitch_median_PSD_forLI_H1.dat"
                ]
            return []

        mock_glob.side_effect = glob_side_effect
        mock_exists.return_value = True

        pipeline = BayesWave(mock_production)
        assets = pipeline.collect_assets()

        assert "psds" in assets
        assert "xml psds" in assets
        assert "H1" in assets["psds"]
        assert assets["reconstructions"] == {}
        assert assets["bayes factors"] == {}
        assert "skymap" not in assets

    @patch("asimov_bayeswave.bayeswave.glob.glob")
    def test_collect_assets_no_psds(self, mock_glob, mock_production, mock_config):
        """Regression test for a real bug: collect_assets() used to leave
        `asset` as the raw (empty) glob.glob() result and call
        os.path.exists(asset) unconditionally -- os.path.exists() raises
        TypeError on a list argument, so this crashed on every single
        detect_completion() poll before the job finished (i.e. almost
        always, since that's the normal case while a job is still running),
        not just at the end. Deliberately does NOT mock os.path.exists
        here, unlike test_collect_assets_psds above, so this exercises the
        real code path that used to crash."""
        mock_glob.return_value = []

        pipeline = BayesWave(mock_production)
        assets = pipeline.collect_assets()  # must not raise TypeError

        assert "psds" in assets
        assert len(assets["psds"]) == 0

    def test_detect_completion_before_job_finishes_does_not_crash(
        self, mock_production, mock_config
    ):
        """End-to-end version of the regression above, through the public
        detect_completion() API a real monitoring loop actually calls, and
        against a genuinely empty rundir (no mocking of glob or
        os.path.exists at all) rather than a real production's PSDs simply
        not existing yet."""
        pipeline = BayesWave(mock_production)
        assert pipeline.detect_completion() is False


class TestDetectCompletion:
    """Test completion detection."""

    @patch("asimov_bayeswave.bayeswave.BayesWave.collect_assets")
    def test_detect_completion_success(
        self, mock_collect, mock_production, mock_config
    ):
        """Test successful completion detection."""
        mock_collect.return_value = {"psds": {"H1": "/path/to/psd.dat"}}

        pipeline = BayesWave(mock_production)
        assert pipeline.detect_completion() is True

    @patch("asimov_bayeswave.bayeswave.BayesWave.collect_assets")
    def test_detect_completion_no_psds(
        self, mock_collect, mock_production, mock_config
    ):
        """Test completion detection with no PSDs."""
        mock_collect.return_value = {"psds": {}}

        pipeline = BayesWave(mock_production)
        assert pipeline.detect_completion() is False


class TestSupressPsd:
    """Test PSD suppression."""

    @patch("asimov_bayeswave.bayeswave.np.savetxt")
    @patch("asimov_bayeswave.bayeswave.np.genfromtxt")
    @patch("asimov_bayeswave.bayeswave.copyfile")
    @patch("asimov_bayeswave.bayeswave.Store")
    def test_supress_psd(
        self,
        mock_store,
        mock_copy,
        mock_genfromtxt,
        mock_savetxt,
        mock_production,
        mock_config,
    ):
        """Test PSD suppression functionality."""
        # Create mock PSD data
        freq = np.linspace(10, 100, 100)
        psd = np.ones_like(freq) * 1e-23
        mock_psd_data = np.column_stack((freq, psd))
        mock_genfromtxt.return_value = mock_psd_data

        # Mock store
        mock_store_instance = MagicMock()
        mock_store.return_value = mock_store_instance

        pipeline = BayesWave(mock_production)
        pipeline.supress_psd("H1", 60.0, 60.5)

        # Verify suppression was applied
        assert mock_savetxt.called
        call_args = mock_savetxt.call_args
        suppressed_data = call_args[0][1]

        # Check that frequencies in the suppression range have PSD = 1.0
        freq_mask = (suppressed_data[:, 0] >= 60.0) & (suppressed_data[:, 0] <= 60.5)
        assert np.all(suppressed_data[freq_mask, 1] == 1.0)


class TestConvertPsd:
    """Test PSD conversion to XML.

    Regression coverage for a real bug: _convert_psd used to run
    convert_psd_ascii2xml with stderr=subprocess.STDOUT (merging stderr
    into stdout) and then branch failure detection on the *separately*
    captured stderr value from communicate() -- which is always None/empty
    whenever stderr is merged into stdout like that, so the failure branch
    could never actually trigger, regardless of the real exit status. These
    tests explicitly set `.returncode` (which the fixed code now checks)
    rather than relying on a `.communicate()` stderr value.
    """

    @patch("asimov_bayeswave.bayeswave.subprocess.Popen")
    def test_convert_psd_success(self, mock_popen, mock_production, mock_config):
        """Test successful PSD conversion."""
        mock_process = Mock()
        mock_process.communicate.return_value = (b"Conversion successful", None)
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        mock_production.event.repository.add_file = Mock()

        pipeline = BayesWave(mock_production)
        pipeline._convert_psd("/path/to/psd.dat", "H1")

        assert mock_popen.called
        call_args = mock_popen.call_args[0][0]
        assert "convert_psd_ascii2xml" in call_args
        mock_production.event.repository.add_file.assert_called_once()

    @patch("asimov_bayeswave.bayeswave.subprocess.Popen")
    def test_convert_psd_failure_detected_from_returncode(
        self, mock_popen, mock_production, mock_config
    ):
        """A non-zero exit status (with stdout/stderr merged, as the real
        subprocess call configures it) must be detected as a failure."""
        mock_process = Mock()
        mock_process.communicate.return_value = (b"some error output", None)
        mock_process.returncode = 1
        mock_popen.return_value = mock_process

        pipeline = BayesWave(mock_production)

        with pytest.raises(
            PipelineException, match="XML format PSD could not be created"
        ):
            pipeline._convert_psd("/path/to/psd.dat", "H1")
        assert mock_production.status == "stuck"

    @patch("asimov_bayeswave.bayeswave.subprocess.Popen")
    def test_convert_psd_success_not_falsely_flagged_by_empty_stderr(
        self, mock_popen, mock_production, mock_config
    ):
        """The specific shape of the old bug: a successful run whose merged
        stderr happens to be empty/None must NOT be (mis)treated as a
        success by accident of that emptiness -- it must be judged by
        returncode, which this test sets to 0 explicitly."""
        mock_process = Mock()
        mock_process.communicate.return_value = (b"", None)
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        mock_production.event.repository.add_file = Mock()

        pipeline = BayesWave(mock_production)
        pipeline._convert_psd("/path/to/psd.dat", "H1")  # must not raise

    @patch(
        "asimov_bayeswave.bayeswave.subprocess.Popen",
        side_effect=FileNotFoundError("no such file"),
    )
    def test_convert_psd_missing_executable_raises_clear_exception(
        self, mock_popen, mock_production, mock_config
    ):
        """convert_psd_ascii2xml is not shipped by any current public
        conda-forge package (bayeswave, bayeswaveutils, lalinference and
        lalapps were all checked while developing this plugin's e2e test);
        a bare FileNotFoundError should not propagate uncaught."""
        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="convert_psd_ascii2xml"):
            pipeline._convert_psd("/path/to/psd.dat", "H1")


class TestAfterCompletion:
    """Test post-completion processing, in particular that XML PSD
    conversion is optional rather than a hard dependency.

    convert_psd_ascii2xml ships with RIFT, not BayesWave -- this plugin
    deliberately does not depend on RIFT, so the executable may genuinely
    not be installed. after_completion() must check for it upfront and
    skip the XML conversion step cleanly in that case, rather than treating
    a routine, expected absence as a failure on every single completion.
    """

    @patch("asimov_bayeswave.bayeswave.shutil.which", return_value=None)
    @patch("asimov_bayeswave.bayeswave.BayesWave._convert_psd")
    @patch("asimov_bayeswave.bayeswave.BayesWave.collect_pages")
    @patch("asimov_bayeswave.bayeswave.BayesWave.store_assets")
    @patch("asimov_bayeswave.bayeswave.BayesWave.collect_assets")
    def test_skips_xml_conversion_when_executable_missing(
        self,
        mock_collect_assets,
        mock_store_assets,
        mock_collect_pages,
        mock_convert_psd,
        mock_which,
        mock_production,
        mock_config,
    ):
        mock_collect_assets.return_value = {
            "psds": {"H1": "/path/to/H1-psd.dat", "L1": "/path/to/L1-psd.dat"}
        }

        pipeline = BayesWave(mock_production)
        pipeline.after_completion()

        mock_convert_psd.assert_not_called()
        assert mock_production.status == "uploaded"

    @patch(
        "asimov_bayeswave.bayeswave.shutil.which",
        return_value="/usr/bin/convert_psd_ascii2xml",
    )
    @patch("asimov_bayeswave.bayeswave.BayesWave._convert_psd")
    @patch("asimov_bayeswave.bayeswave.BayesWave.collect_pages")
    @patch("asimov_bayeswave.bayeswave.BayesWave.store_assets")
    @patch("asimov_bayeswave.bayeswave.BayesWave.collect_assets")
    def test_converts_xml_when_executable_available(
        self,
        mock_collect_assets,
        mock_store_assets,
        mock_collect_pages,
        mock_convert_psd,
        mock_which,
        mock_production,
        mock_config,
    ):
        mock_collect_assets.return_value = {
            "psds": {"H1": "/path/to/H1-psd.dat", "L1": "/path/to/L1-psd.dat"}
        }

        pipeline = BayesWave(mock_production)
        pipeline.after_completion()

        assert mock_convert_psd.call_count == 2
        mock_convert_psd.assert_any_call(ascii_format="/path/to/H1-psd.dat", ifo="H1")
        mock_convert_psd.assert_any_call(ascii_format="/path/to/L1-psd.dat", ifo="L1")
        assert mock_production.status == "uploaded"


class TestResurrect:
    """Test job resurrection."""

    @patch("asimov_bayeswave.bayeswave.BayesWave.submit_dag")
    @patch("asimov_bayeswave.bayeswave.glob.glob")
    def test_resurrect_with_rescue_files(
        self, mock_glob, mock_submit, mock_production, mock_config
    ):
        """Test resurrection with rescue files."""
        mock_glob.return_value = ["rescue001", "rescue002"]

        pipeline = BayesWave(mock_production)
        pipeline.resurrect()

        assert mock_submit.called

    @patch("asimov_bayeswave.bayeswave.BayesWave.submit_dag")
    @patch("asimov_bayeswave.bayeswave.glob.glob")
    def test_resurrect_too_many_attempts(
        self, mock_glob, mock_submit, mock_production, mock_config
    ):
        """Test resurrection fails after too many attempts."""
        mock_glob.return_value = [f"rescue{i:03d}" for i in range(1, 6)]

        pipeline = BayesWave(mock_production)
        pipeline.resurrect()

        assert not mock_submit.called


class TestHtml:
    """Test HTML output generation."""

    def test_html_finished_status(self, mock_production, mock_config):
        """Test HTML generation for finished job."""
        mock_production.status = "finished"

        pipeline = BayesWave(mock_production)
        html = pipeline.html()

        assert "asimov-pipeline" in html
        assert "Megaplot" in html
        assert mock_production.name in html

    def test_html_running_status(self, mock_production, mock_config):
        """Test HTML generation for running job."""
        mock_production.status = "running"

        pipeline = BayesWave(mock_production)
        html = pipeline.html()

        assert html == ""


class TestCollectLogs:
    """Regression test for a real bug: collect_logs() read
    config.get("logging", "directory"), but "directory" is not a real
    asimov.conf option -- every real caller (asimov/__init__.py,
    asimov/project.py, asimov/analysis.py, and this pipeline's own
    corrected code) uses config.get("logging", "location") (see
    asimov/asimov.conf: `[logging]` / `location = logs`). The old key
    would raise configparser.NoOptionError against a real asimov config,
    so collect_logs() has always been broken in real usage."""

    def test_reads_the_real_logging_location_key(
        self, mock_production, mock_config, tmp_path
    ):
        log_dir = tmp_path / mock_production.event.name / mock_production.name
        log_dir.mkdir(parents=True)
        (log_dir / "asimov.log").write_text("hello from the production log")

        mock_config.get = lambda section, key: (
            str(tmp_path) if (section, key) == ("logging", "location") else ""
        )

        pipeline = BayesWave(mock_production)
        with patch("asimov_bayeswave.bayeswave.glob.glob", return_value=[]):
            messages = pipeline.collect_logs()

        assert messages["production"] == "hello from the production log"


def test_module_imports():
    """Test that the module imports correctly."""
    from asimov_bayeswave import BayesWave, __version__

    assert BayesWave is not None
    assert __version__ is not None


class TestComponentsResolution:
    """Test resolution of likelihood.components (+ coherence test) into a
    run mode, per the design agreed for issue #2: no likelihood.components
    and no coherence test must resolve to exactly today's PSD-only
    behaviour; everything else is a new, additive mode."""

    def test_no_components_is_psd_only(self, mock_production, mock_config):
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "psd"
        assert pipeline.model_flags == ["cleanOnly"]
        assert pipeline.bayesline_enabled is True

    def test_explicit_none_none_is_psd_only(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "none",
            "glitch": "none",
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "psd"
        assert pipeline.model_flags == ["cleanOnly"]

    def test_components_present_defaults_to_full(self, mock_production, mock_config):
        """An (otherwise empty) components block defaults signal and
        glitch to 'wavelets' each, per the vocabulary's documented
        defaults."""
        mock_production.meta["likelihood"]["components"] = {}
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "full"
        assert pipeline.model_flags == ["fullOnly"]

    def test_signal_only(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "none",
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "signal"
        assert pipeline.model_flags == ["signalOnly"]

    def test_glitch_only(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "none",
            "glitch": "wavelets",
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "glitch"
        assert pipeline.model_flags == ["glitchOnly"]

    def test_signal_and_glitch_is_full(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "wavelets",
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "full"
        assert pipeline.model_flags == ["fullOnly"]

    def test_chirplets_flag_added(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "chirplets",
            "glitch": "none",
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "signal"
        assert pipeline.model_flags == ["signalOnly", "chirplets"]

    def test_coherence_test_alone_implies_full(self, mock_production, mock_config):
        """coherence test: true with no components block at all must still
        switch out of the PSD-only default, defaulting signal and glitch
        to 'wavelets'."""
        mock_production.meta["likelihood"]["coherence test"] = True
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "full"

    def test_coherence_test_does_not_override_explicit_components(
        self, mock_production, mock_config
    ):
        mock_production.meta["likelihood"]["coherence test"] = True
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "none",
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.run_mode == "signal"

    def test_noise_lines_false_disables_bayesline(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "noise": {"lines": False},
        }
        pipeline = BayesWave(mock_production)
        assert pipeline.bayesline_enabled is False

    def test_unknown_signal_value_raises(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {"signal": "nonsense"}
        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="signal"):
            pipeline.run_mode

    def test_unknown_glitch_value_raises(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {"glitch": "nonsense"}
        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="glitch"):
            pipeline.run_mode

    def test_unknown_noise_psd_value_raises(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "noise": {"psd": "nonsense"}
        }
        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="noise.psd"):
            pipeline.run_mode

    def test_signal_cbc_raises_not_yet_supported(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {"signal": "cbc"}
        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="not yet supported"):
            pipeline.run_mode

    def test_noise_psd_fixed_raises_not_yet_supported(
        self, mock_production, mock_config
    ):
        mock_production.meta["likelihood"]["components"] = {"noise": {"psd": "fixed"}}
        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="not yet supported"):
            pipeline.run_mode


class TestBuildDagValidation:
    """build_dag() must reject unsupported/unknown component combinations
    before attempting to build a DAG."""

    @patch("asimov_bayeswave.bayeswave.shutil.which")
    def test_build_dag_rejects_signal_cbc(
        self, mock_which, mock_production, mock_config
    ):
        mock_which.return_value = "/opt/conda/bin/bayeswave_pipe"
        mock_production.meta["likelihood"]["components"] = {"signal": "cbc"}
        mock_ini = MagicMock()
        mock_ini.ini_loc = "/tmp/test.ini"
        mock_production.get_configuration.return_value = mock_ini

        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="not yet supported"):
            pipeline.build_dag(dryrun=True)

    @patch("asimov_bayeswave.bayeswave.shutil.which")
    def test_build_dag_rejects_fixed_psd(
        self, mock_which, mock_production, mock_config
    ):
        mock_which.return_value = "/opt/conda/bin/bayeswave_pipe"
        mock_production.meta["likelihood"]["components"] = {"noise": {"psd": "fixed"}}
        mock_ini = MagicMock()
        mock_ini.ini_loc = "/tmp/test.ini"
        mock_production.get_configuration.return_value = mock_ini

        pipeline = BayesWave(mock_production)
        with pytest.raises(PipelineException, match="not yet supported"):
            pipeline.build_dag(dryrun=True)


class TestTemplateRendering:
    """Render the bundled Liquid ini template the way asimov's
    Analysis.make_config() does (production=, pipeline=, config=), and
    check the rendered [bayeswave_options]/[bayeswave_post_options]
    sections for each components combination."""

    def _render(self, mock_production, mock_config):
        pipeline = BayesWave(mock_production)
        template = Liquid(CONFIG_TEMPLATE)
        rendered = template.render(
            production=mock_production,
            pipeline=pipeline,
            config=mock_config,
        )
        options = rendered[
            rendered.index("[bayeswave_options]") : rendered.index(
                "[bayeswave_post_options]"
            )
        ]
        post_options = rendered[
            rendered.index("[bayeswave_post_options]") : rendered.index("[condor]")
        ]
        return options, post_options

    def test_default_renders_cleanonly_psd_run(self, mock_production, mock_config):
        """Regression test: with no likelihood.components at all, the
        rendered ini must contain exactly the same options it always has
        -- cleanOnly and bayesLine in both sections, nothing else new."""
        options, post_options = self._render(mock_production, mock_config)

        assert "cleanOnly=" in options
        assert "signalOnly" not in options
        assert "glitchOnly" not in options
        assert "fullOnly" not in options
        assert "chirplets" not in options
        assert "bayesLine =" in options
        assert "0noise=" in post_options
        assert "lite =" in post_options
        assert "bayesLine =" in post_options

    def test_full_mode_renders_fullonly(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "wavelets",
        }
        options, post_options = self._render(mock_production, mock_config)

        assert "fullOnly=" in options
        assert "cleanOnly" not in options
        # --0noise/--lite are safe (and correct) for every run mode -- see
        # BayesWave.model_flags/bayesline_enabled docstrings.
        assert "0noise=" in post_options
        assert "lite =" in post_options

    def test_signal_only_mode_renders_signalonly(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "none",
        }
        options, _ = self._render(mock_production, mock_config)
        assert "signalOnly=" in options
        assert "glitchOnly" not in options

    def test_glitch_only_mode_renders_glitchonly(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "none",
            "glitch": "wavelets",
        }
        options, _ = self._render(mock_production, mock_config)
        assert "glitchOnly=" in options
        assert "signalOnly" not in options

    def test_chirplets_rendered(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "chirplets",
            "glitch": "none",
        }
        options, _ = self._render(mock_production, mock_config)
        assert "signalOnly=" in options
        assert "chirplets=" in options

    def test_lines_false_omits_bayesline(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["components"] = {
            "noise": {"lines": False},
        }
        options, post_options = self._render(mock_production, mock_config)
        assert "bayesLine" not in options
        assert "bayesLine" not in post_options

    def test_kwargs_rendered_as_extra_options(self, mock_production, mock_config):
        mock_production.meta["likelihood"]["kwargs"] = {
            "waveletFrac": "0.5",
        }
        options, _ = self._render(mock_production, mock_config)
        assert "waveletFrac = 0.5" in options


class TestDetectCompletionModes:
    """Test that completion detection is correct per run mode, including
    the early-completion regression the "clean phase finishes first"
    behaviour would otherwise cause."""

    def test_psd_mode_uses_psds_only(self, mock_production, mock_config):
        pipeline = BayesWave(mock_production)
        with patch.object(
            BayesWave, "collect_assets", return_value={"psds": {"H1": "/x.dat"}}
        ):
            assert pipeline.detect_completion() is True

    def test_full_mode_not_complete_when_only_clean_has_finished(
        self, mock_production, mock_config
    ):
        """Regression test: in a signal+glitch run, the "clean"
        PSD-estimation phase finishes well before the rest of
        post-processing does. If detect_completion() looked only at PSDs
        (as it always has), it would report completion far too early."""
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "wavelets",
        }
        pipeline = BayesWave(mock_production)

        # Only the clean-phase PSDs exist so far -- the signal/glitch
        # reconstructions (and the final megaplot page) have not been
        # produced yet.
        with patch.object(
            BayesWave,
            "collect_assets",
            return_value={
                "psds": {"H1": "/x.dat", "L1": "/y.dat"},
                "reconstructions": {},
                "bayes factors": {},
            },
        ):
            assert pipeline.detect_completion() is False

    def test_full_mode_complete_when_all_reconstructions_and_megaplot_exist(
        self, mock_production, mock_config
    ):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "wavelets",
        }
        pipeline = BayesWave(mock_production)

        with (
            patch.object(
                BayesWave,
                "collect_assets",
                return_value={
                    "psds": {"H1": "/x.dat", "L1": "/y.dat"},
                    "reconstructions": {
                        "signal": {"H1": "/s_h1.dat", "L1": "/s_l1.dat"},
                        "glitch": {"H1": "/g_h1.dat", "L1": "/g_l1.dat"},
                    },
                    "bayes factors": {"signal:noise": 12.3},
                },
            ),
            patch(
                "asimov_bayeswave.bayeswave.glob.glob",
                return_value=["/tmp/test_rundir/trigtime_123/index.html"],
            ),
        ):
            assert pipeline.detect_completion() is True

    def test_full_mode_not_complete_without_megaplot_page(
        self, mock_production, mock_config
    ):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "wavelets",
        }
        pipeline = BayesWave(mock_production)

        with (
            patch.object(
                BayesWave,
                "collect_assets",
                return_value={
                    "psds": {"H1": "/x.dat", "L1": "/y.dat"},
                    "reconstructions": {
                        "signal": {"H1": "/s_h1.dat", "L1": "/s_l1.dat"},
                        "glitch": {"H1": "/g_h1.dat", "L1": "/g_l1.dat"},
                    },
                    "bayes factors": {},
                },
            ),
            patch("asimov_bayeswave.bayeswave.glob.glob", return_value=[]),
        ):
            assert pipeline.detect_completion() is False

    def test_signal_only_mode_ignores_missing_glitch_reconstruction(
        self, mock_production, mock_config
    ):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "none",
        }
        pipeline = BayesWave(mock_production)

        with (
            patch.object(
                BayesWave,
                "collect_assets",
                return_value={
                    "psds": {},
                    "reconstructions": {
                        "signal": {"H1": "/s_h1.dat", "L1": "/s_l1.dat"},
                    },
                    "bayes factors": {},
                },
            ),
            patch(
                "asimov_bayeswave.bayeswave.glob.glob",
                return_value=["/tmp/test_rundir/trigtime_123/index.html"],
            ),
        ):
            assert pipeline.detect_completion() is True


class TestCollectAssetsExtended:
    """Test collect_assets() against real (fake) trigtime_*/post/{clean,
    signal,glitch,full}/ trees, plus evidence.dat and a skymap, rather than
    mocking glob."""

    def _make_run_tree(self, tmp_path, mock_production, mode="full"):
        mock_production.rundir = str(tmp_path)
        trigdir = tmp_path / "trigtime_123.000000000_H1L1"
        trigdir.mkdir()

        clean_dir = trigdir / "post" / "clean"
        clean_dir.mkdir(parents=True)
        for det in ("H1", "L1"):
            (clean_dir / f"glitch_median_PSD_forLI_{det}.dat").write_text("1 2\n")

        if mode in ("signal", "full"):
            signal_dir = trigdir / "post" / ("full" if mode == "full" else "signal")
            signal_dir.mkdir(parents=True, exist_ok=True)
            for det in ("H1", "L1"):
                (
                    signal_dir / f"signal_median_time_domain_waveform_{det}.dat"
                ).write_text("0.0 1.0\n")

        if mode in ("glitch", "full"):
            glitch_dir = trigdir / "post" / ("full" if mode == "full" else "glitch")
            glitch_dir.mkdir(parents=True, exist_ok=True)
            for det in ("H1", "L1"):
                (
                    glitch_dir / f"glitch_median_time_domain_waveform_{det}.dat"
                ).write_text("0.0 0.5\n")

        (trigdir / "evidence.dat").write_text(
            "signal 12.5 0.2\nglitch 3.1 0.1\nnoise 0.0 0.0\n"
        )

        plots_dir = trigdir / "plots"
        plots_dir.mkdir()
        (plots_dir / "skymap.png").write_bytes(b"\x89PNG")

        return trigdir

    def test_full_mode_collects_reconstructions_bayes_factors_and_skymap(
        self, mock_production, mock_config, tmp_path
    ):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "wavelets",
        }
        self._make_run_tree(tmp_path, mock_production, mode="full")

        pipeline = BayesWave(mock_production)
        assets = pipeline.collect_assets()

        assert set(assets["reconstructions"]["signal"]) == {"H1", "L1"}
        assert set(assets["reconstructions"]["glitch"]) == {"H1", "L1"}
        assert assets["bayes factors"]["signal:noise"] == pytest.approx(12.5)
        assert assets["bayes factors"]["signal:glitch"] == pytest.approx(12.5 - 3.1)
        assert assets["bayes factors"]["glitch:noise"] == pytest.approx(3.1)
        assert assets["skymap"].endswith("skymap.png")

    def test_signal_only_mode_has_no_glitch_reconstruction(
        self, mock_production, mock_config, tmp_path
    ):
        mock_production.meta["likelihood"]["components"] = {
            "signal": "wavelets",
            "glitch": "none",
        }
        self._make_run_tree(tmp_path, mock_production, mode="signal")

        pipeline = BayesWave(mock_production)
        assets = pipeline.collect_assets()

        assert "signal" in assets["reconstructions"]
        assert "glitch" not in assets["reconstructions"]

    def test_psd_mode_has_no_reconstructions_or_skymap(
        self, mock_production, mock_config, tmp_path
    ):
        mock_production.rundir = str(tmp_path)
        trigdir = tmp_path / "trigtime_123.000000000_H1L1"
        clean_dir = trigdir / "post" / "clean"
        clean_dir.mkdir(parents=True)
        for det in ("H1", "L1"):
            (clean_dir / f"glitch_median_PSD_forLI_{det}.dat").write_text("1 2\n")

        pipeline = BayesWave(mock_production)
        assets = pipeline.collect_assets()

        assert assets["reconstructions"] == {}
        assert assets["bayes factors"] == {}
        assert "skymap" not in assets


class TestParseBayesFactors:
    """Test _parse_bayes_factors() directly against evidence.dat-shaped
    input, per the format verified against BayesWave.c (fprintf(evidence,
    "signal %.12g %lg\\n", ...) etc)."""

    def test_parses_all_three_pairs(self, tmp_path):
        evidence_file = tmp_path / "evidence.dat"
        evidence_file.write_text("signal 10.0 0.1\nglitch 4.0 0.1\nnoise 1.0 0.1\n")

        bayes_factors = BayesWave._parse_bayes_factors(str(evidence_file))

        assert bayes_factors["signal:noise"] == pytest.approx(9.0)
        assert bayes_factors["signal:glitch"] == pytest.approx(6.0)
        assert bayes_factors["glitch:noise"] == pytest.approx(3.0)

    def test_missing_models_are_omitted(self, tmp_path):
        evidence_file = tmp_path / "evidence.dat"
        evidence_file.write_text("signal 10.0 0.1\n")

        bayes_factors = BayesWave._parse_bayes_factors(str(evidence_file))

        assert bayes_factors == {}

    def test_ignores_malformed_lines(self, tmp_path):
        evidence_file = tmp_path / "evidence.dat"
        evidence_file.write_text("signal 10.0 0.1\nsomething odd\nnoise 1.0 0.1\n")

        bayes_factors = BayesWave._parse_bayes_factors(str(evidence_file))

        assert bayes_factors["signal:noise"] == pytest.approx(9.0)


class TestStoreReconstructions:
    """Test store_reconstructions(), the PSD-store-style helper that
    commits reconstruction files to the event repository and the Asimov
    store."""

    @patch("asimov_bayeswave.bayeswave.Store")
    def test_stores_each_component_and_ifo(
        self, mock_store, mock_production, mock_config
    ):
        mock_store_instance = MagicMock()
        mock_store.return_value = mock_store_instance
        mock_production.event.repository.add_file = Mock()

        pipeline = BayesWave(mock_production)
        with patch.object(
            BayesWave,
            "collect_assets",
            return_value={
                "reconstructions": {
                    "signal": {"H1": "/tmp/signal_H1.dat"},
                    "glitch": {"H1": "/tmp/glitch_H1.dat"},
                }
            },
        ):
            pipeline.store_reconstructions()

        assert mock_production.event.repository.add_file.call_count == 2
        assert mock_store_instance.add_file.call_count == 2

    @patch("asimov_bayeswave.bayeswave.Store")
    def test_one_failure_does_not_stop_the_others(
        self, mock_store, mock_production, mock_config
    ):
        mock_store.return_value = MagicMock()
        mock_production.event.repository.add_file = Mock(
            side_effect=[Exception("boom"), None]
        )

        pipeline = BayesWave(mock_production)
        with patch.object(
            BayesWave,
            "collect_assets",
            return_value={
                "reconstructions": {
                    "signal": {"H1": "/tmp/signal_H1.dat"},
                    "glitch": {"H1": "/tmp/glitch_H1.dat"},
                }
            },
        ):
            pipeline.store_reconstructions()  # must not raise

        assert mock_production.event.repository.add_file.call_count == 2


class TestHtmlExtended:
    """Test html() additions: reconstruction plots, a Bayes-factor table
    and a skymap image, only when present in production.meta."""

    def test_psd_only_html_unchanged(self, mock_production, mock_config):
        mock_production.status = "finished"
        pipeline = BayesWave(mock_production)
        html = pipeline.html()

        assert "signal_waveform" not in html
        assert "glitch_waveform" not in html
        assert "skymap" not in html
        assert "Bayes factor" not in html

    def test_reconstructions_and_bayes_factors_and_skymap_shown(
        self, mock_production, mock_config
    ):
        mock_production.status = "finished"
        mock_production.meta["reconstructions"] = {
            "signal": {"H1": "/x"},
            "glitch": {"H1": "/y"},
        }
        mock_production.meta["bayes factors"] = {"signal:noise": 12.3}
        mock_production.meta["skymap"] = "/tmp/skymap.png"

        pipeline = BayesWave(mock_production)
        html = pipeline.html()

        assert "signal_waveform_H1.png" in html
        assert "glitch_waveform_H1.png" in html
        assert "skymap.png" in html
        assert "signal:noise" in html
        assert "12.30" in html
