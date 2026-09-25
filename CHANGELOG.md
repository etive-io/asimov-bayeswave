# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Signal and glitch reconstructions (issue #2): a production can now request BayesWave's
  signal and/or glitch wavelet models, not just on-source PSD estimation, via the
  pipeline-agnostic `likelihood.components` ledger term (`signal`/`glitch`:
  `none`/`wavelets`/`chirplets`, plus `noise.psd`/`noise.lines`), and the pre-existing
  `likelihood.coherence test` term. No `bayeswave:`-named blueprint section is introduced.
  - New `BayesWave.run_mode` (`"psd"`/`"signal"`/`"glitch"`/`"full"`), `model_flags` and
    `bayesline_enabled` properties drive the bundled `configs/bayeswave.ini` template, so
    the default rendering (no `likelihood.components` given) is unchanged byte-for-byte
    other than added comments -- verified with a regression test.
  - `build_dag()` now validates `likelihood.components` up front and raises a clear
    `PipelineException` for combinations BayesWave can't (yet) run (`signal: cbc`,
    `noise.psd: fixed`) or unknown values, instead of mis-running or ignoring them.
  - `detect_completion()` now depends on the requested run mode: unchanged for a PSD-only
    production, but for a signal/glitch/full production it requires the reconstruction for
    every requested model and interferometer, plus the final megaplot page, rather than
    just the PSDs -- the PSD-producing "clean" phase finishes well before the rest of
    post-processing does, so checking PSDs alone would report completion far too early.
  - `collect_assets()` now also returns `"reconstructions"` (`{component: {ifo: path}}`),
    `"bayes factors"` (log Bayes factors parsed from BayesWave's `evidence.dat`, e.g.
    `{"signal:noise": ..., "signal:glitch": ..., "glitch:noise": ...}`) and `"skymap"`
    (megaplot.py's `plots/skymap.png`, produced once a signal model has run) when present;
    `"psds"`/`"xml psds"` are unchanged.
  - New `store_reconstructions()`, called from `after_completion()`, commits reconstruction
    files to the event repository and the Asimov store the same way `store_assets()` does
    for PSDs; Bayes factors are merged into `production.meta` the same way PSDs already are.
  - `html()` now also renders reconstruction plots, a log Bayes-factor table and a sky map
    image when present in `production.meta`; unchanged for a PSD-only production.
  - The BayesWave flag/output mapping this relies on (`--cleanOnly`/`--signalOnly`/
    `--glitchOnly`/`--fullOnly`/`--chirplets`/`--bayesLine`, the `post/<model>/` output
    layout, `evidence.dat`'s format and which options `bayeswave_pipe` auto-forwards from
    `[bayeswave_options]` to `[bayeswave_post_options]`) was verified against the BayesWave
    source (`git.ligo.org/lscsoft/bayeswave`), not guessed -- see the plugin's docstrings
    and inline comments for the specific source locations.
- Initial release of asimov-bayeswave plugin
- BayesWave pipeline integration for Asimov 0.7+
- Automatic PSD generation and collection
- XML format PSD conversion
- HTCondor DAG generation and submission
- Post-processing and result collection
- PSD suppression capabilities
- Megaplot output collection
- Comprehensive test suite
- Sphinx documentation with kentigern theme
- GitHub Actions CI/CD workflows
- `[asimov]` optional dependency group for explicit asimov integration
- `config_template` property, pointing at the bundled
  `asimov_bayeswave/configs/bayeswave.ini`, so `asimov manage build` can render a
  production's `.ini` directly from ledger meta-data (this property was entirely missing;
  the bundled template existed but nothing pointed at it -- see Fixed, below)
- A genuine end-to-end test (`.github/workflows/e2e.yml`): real `bayeswave_pipe` DAG
  generation and real HTCondor execution (`BayesWave` clean run, `BayesWavePost`,
  `megaplot.py`) against real (trimmed) GW150914 H1 GWOSC strain, waiting for a real
  `glitch_median_PSD_forLI_H1.dat` file -- not a smoke test. Also regression-checks that
  a production reaches a genuinely finished/uploaded state (see Fixed) and that the
  missing `convert_psd_ascii2xml` tool (see Fixed) is handled gracefully rather than
  crashing post-processing

### Changed
- Extracted BayesWave integration from Asimov core into standalone plugin
- Removed deprecation warning from Asimov 0.6
- Updated version constraint to require asimov>=0.7
- Added installation instructions for asimov[gw]
- `submit_dag()` now uses `self.scheduler.submit_dag(...)` (asimov's scheduler-agnostic
  HTCondor/Slurm API) instead of shelling out to `condor_submit_dag` directly and
  regex-scraping its stdout, matching the pattern used in the sibling
  `asimov-lalinference` plugin; this also gets Slurm support for free. The DAG filename
  passed to it is now computed from `os.path.basename(production.rundir)` -- confirmed,
  by reading `bayeswave_pipe` itself, to be exactly what it names the generated top-level
  DAG file, rather than assumed from `production.name` (the two are almost always the
  same string, since that's asimov's own default rundir convention, but the former is
  correct even when that convention is overridden)
- `build_dag()` now resolves the `bayeswave_pipe` executable via `shutil.which()`,
  falling back from any configured/explicit path to a bare `bayeswave_pipe` on `$PATH`,
  and raises a clear `PipelineException` if neither can be found, rather than silently
  handing HTCondor a path that may not exist inside minimal/containerised execution
  environments
- `build_dag()` now emits the `--igwn-pool` flag instead of the deprecated `--osg-deploy`
  (confirmed via `bayeswave_pipe --help`: the old flag still works but is reported as
  "OUTDATED. please use --igwn-pool instead")

### Fixed
- Updated dependency constraint to support asimov 0.7 (changed from `asimov>=0.6.0` to `asimov>=0.7`)
- **`collect_assets()` crashed on every single monitoring poll before a job finished**,
  not just at the end: whenever a detector's PSD glob had no matches yet (the normal case
  while a job is still running -- which is the entire point of `detect_completion()`
  polling it repeatedly), the loop left `asset` as that empty list and then called
  `os.path.exists(asset)` unconditionally. `os.path.exists()` raises `TypeError` on a
  list argument, so this crashed immediately, every time, until the PSD file finally
  appeared. The pre-existing unit test suite never caught this because it mocked
  `os.path.exists` to always return `True`. Found and confirmed via the real asimov CLI
  while building the e2e test in this release, exactly as intended.
- **`build_dag()` unconditionally called `RunConfiguration` methods that no longer
  exist**: `ini._get_user()`, `ini.update_accounting(user)`, `ini.set_queue(queue)` and
  `ini.save()` were all removed when `asimov.ini.RunConfiguration` was trimmed down to a
  bare `ConfigParser` wrapper (`.ini_loc` / `.ini` only) for asimov's current core
  release. This meant `build_dag()` raised `AttributeError` for essentially any
  production whose event has a real git repository -- i.e. real usage -- immediately
  after the ini was located. The accounting group and user were already being baked into
  the ini correctly by the Liquid `config_template` at render time
  (`accounting-group-user = {{ config['condor']['user'] }}`), so the dead
  post-processing call is simply removed rather than reimplemented; there is also no
  "queue" concept anywhere in `bayeswave_pipe`'s own ini schema. Found via the real
  `asimov` CLI (`asimov manage build submit`), not caught by unit tests because they
  mock `get_configuration()` with a `MagicMock` that silently accepts any attribute
  access
- **Pipeline construction-order bug**: `__init__` used to eagerly evaluate `self.flow`
  (which needs `likelihood.minimum frequency`) to pre-cache
  `production.meta["quality"]["lowest minimum frequency"]` for the ini template. Pipeline
  construction happens inside `Analysis.__init__`, before `GravitationalWaveTransient`'s
  own `quality` -> `likelihood` migration for a deprecated `quality.minimum frequency`
  blueprint has run, so this eager read could see pre-migration state and raise
  incorrectly. The eager cache is removed entirely; the ini template now calls
  `pipeline.flow` directly (computed fresh on every render), and `flow` itself now raises
  a clear `ValueError` instead of an opaque `AttributeError` when
  `likelihood.minimum frequency` isn't a non-empty dict. (This exact bug and fix
  previously landed in asimov core's own `bayeswave.py`, before it was removed there in
  favour of this plugin package -- this release ports that fix across.)
- `_convert_psd()`'s failure detection was structurally broken: the subprocess was run
  with `stderr=subprocess.STDOUT` (merging stderr into stdout), but failure was judged by
  branching on the *separately*-captured `communicate()` stderr value -- which is always
  `None`/empty whenever stderr has been merged like that, so the failure branch could
  never actually trigger regardless of the real exit status. Failure is now judged from
  the subprocess's actual `returncode`. Separately, `convert_psd_ascii2xml` itself is not
  shipped by any current public conda-forge package (`bayeswave`, `bayeswaveutils`,
  `lalinference` and `lalapps` were all checked while building the e2e test) -- a
  `FileNotFoundError` there now raises a clear `PipelineException` instead of propagating
  bare; `after_completion()`'s existing broad exception handling means this doesn't crash
  post-processing, it just means no XML-format PSD is produced against current public
  tooling (the ascii-format PSD is unaffected)
- `collect_logs()` read `config.get("logging", "directory")`, which is not a real
  `asimov.conf` option (confirmed against the real config module); every real caller
  including asimov core itself uses `config.get("logging", "location")`, which
  `collect_logs()` now uses too
- `upload_assets()` iterated `for detector, asset in self.collect_assets()["psds"]:` --
  iterating a `dict` directly yields only its keys, so this silently unpacked each
  two-character IFO name string (e.g. `"H1"`) into `detector, asset` instead of raising;
  it now iterates `.items()`
- `after_completion()` indexed `self.production.meta["quality"]` directly, which raises
  `KeyError` if a production's metadata has no `quality` section at all (increasingly
  likely now that `minimum frequency` has moved to `likelihood`); now uses `.get("quality",
  {})`. Its `collect_pages()` call is now also guarded against `IndexError` (raised if no
  `trigtime_*` output directory exists yet), not just `FileNotFoundError`
- Packaging: `configs/bayeswave.ini` was listed in `MANIFEST.in` (for sdists) but
  `pyproject.toml` had no `[tool.setuptools.package-data]` entry, so a real (non-editable)
  wheel install would not actually include it -- meaning the new `config_template`
  property would have pointed at a file that doesn't exist once truly installed. Added
  `[tool.setuptools.package-data]`, matching the sibling `asimov-lalinference` plugin
- `after_completion()` always attempted XML PSD conversion via `convert_psd_ascii2xml`,
  which ships with RIFT, not BayesWave -- this plugin deliberately does not depend on
  RIFT, so the tool is routinely absent. The resulting `FileNotFoundError` ->
  `PipelineException` was caught but logged as an `.error()` on every single completion,
  making a normal, expected condition look like a recurring failure. `after_completion()`
  now checks for the executable with `shutil.which()` upfront (matching the pattern
  already used for `bayeswave_pipe` itself in `build_dag()`) and skips XML conversion
  cleanly with an `.info()`-level log when it's absent; the ascii-format PSDs are
  unaffected either way

## [0.1.0] - TBD

### Added
- First public release

[Unreleased]: https://github.com/transientlunatic/asimov-bayeswave/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/transientlunatic/asimov-bayeswave/releases/tag/v0.1.0
