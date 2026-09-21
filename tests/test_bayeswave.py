"""Tests for the BayesWave pipeline integration."""

import os
from unittest.mock import MagicMock, Mock, mock_open, patch

import numpy as np
import pytest
from asimov.pipeline import PipelineException

from asimov_bayeswave import BayesWave


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
        # Mock glob to return PSD files
        mock_glob.return_value = [
            "/tmp/test_rundir/trigtime_123/post/clean/glitch_median_PSD_forLI_H1.dat"
        ]
        mock_exists.return_value = True

        pipeline = BayesWave(mock_production)
        assets = pipeline.collect_assets()

        assert "psds" in assets
        assert "xml psds" in assets
        assert "H1" in assets["psds"]

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
