# asimov-bayeswave

[![Tests](https://github.com/transientlunatic/asimov-bayeswave/actions/workflows/tests.yml/badge.svg)](https://github.com/transientlunatic/asimov-bayeswave/actions/workflows/tests.yml)
[![Documentation Status](https://readthedocs.org/projects/asimov-bayeswave/badge/?version=latest)](https://asimov-bayeswave.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/asimov-bayeswave.svg)](https://badge.fury.io/py/asimov-bayeswave)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

BayesWave pipeline integration for [Asimov](https://git.ligo.org/asimov/asimov).

This package provides a plugin for Asimov 0.7+ that enables integration with the BayesWave parameter estimation pipeline for gravitational wave data analysis.

## Features

- 🔌 **Plugin Architecture**: Seamlessly integrates with Asimov via entry points
- 📊 **PSD Generation**: Automatic power spectral density estimation and collection
- 🌊 **Signal/glitch reconstruction**: Beyond on-source PSD estimation, `likelihood.components`
  can request a signal-only, glitch-only, or combined signal+glitch ("full") BayesWave run,
  collecting waveform reconstructions, log Bayes factors and (for a signal run) a sky map —
  see "Requesting signal/glitch reconstructions" below
- 🔄 **Format Conversion**: Converts PSDs to XML format for use with other pipelines
  (where `convert_psd_ascii2xml` is available — see "Operational notes" below)
- 🚀 **Scheduler-agnostic**: Automated DAG generation and job submission via Asimov's
  HTCondor/Slurm scheduler API
- 📈 **Result Collection**: Automatic collection of megaplot outputs and visualizations
- 🎯 **PSD Suppression**: Support for suppressing frequency bands in PSDs
- 🧪 **Well Tested**: Unit tests plus a genuine end-to-end test (real `bayeswave_pipe` DAG
  generation and HTCondor execution — `BayesWave`, `BayesWavePost`, `megaplot.py` — against
  real GW150914 H1 GWOSC strain, waiting for a real, parseable
  `glitch_median_PSD_forLI_H1.dat`, not a smoke test)

## Installation

### Via Asimov (Recommended)

If you have asimov 0.7+, you can install gravitational wave pipelines including bayeswave with:

```bash
pip install asimov[gw]
```

This will automatically install asimov-bayeswave and other GW analysis plugins.

### From PyPI (when released)

```bash
pip install asimov-bayeswave
```

### From Source

```bash
git clone https://github.com/transientlunatic/asimov-bayeswave.git
cd asimov-bayeswave
pip install -e .
```

### For Development

```bash
pip install -e ".[docs,test]"
```

## Quick Start

Once installed, the BayesWave pipeline is automatically available in Asimov. 
To add a new bayeswave analysis you can create a blueprint YAML file like the following:

```yaml
kind: analysis
pipeline: bayeswave
comment: PSD generation with BayesWave
likelihood:
  sample rate: 2048
  segment length: 8
data:
  channels:
    H1: H1:GDS-CALIB_STRAIN
    L1: L1:GDS-CALIB_STRAIN
quality:
  minimum frequency:
    H1: 20
    L1: 20
```

## Requesting signal/glitch reconstructions

By default (no `likelihood.components` block) a BayesWave analysis only estimates an
on-source PSD, exactly as before. To also (or instead) request BayesWave's signal and/or
glitch wavelet models — for source reconstructions, a coherence test, or both — add a
`likelihood.components` block, following asimov's pipeline-agnostic ledger vocabulary
(nothing pipeline-specific is added to the blueprint):

```yaml
kind: analysis
pipeline: bayeswave
comment: Signal and glitch reconstruction with BayesWave
likelihood:
  sample rate: 2048
  segment length: 8
  minimum frequency:
    H1: 20
    L1: 20
  components:
    signal: wavelets   # none | wavelets | chirplets | cbc (cbc: not yet supported)
    glitch: wavelets   # none | wavelets | chirplets
    noise:
      psd: fit         # fit | fixed (fixed: not yet supported)
      lines: true       # BayesLine spectral-line modelling
  coherence test: true  # implies signal: wavelets, glitch: wavelets if not given
data:
  channels:
    H1: H1:GDS-CALIB_STRAIN
    L1: L1:GDS-CALIB_STRAIN
```

`likelihood.components.signal`/`.glitch` default to `none` (unset entirely, or a bare
`components:` block with neither key given, stays a PSD-only run — e.g.
`components: {noise: {lines: false}}` on its own does *not* turn on signal/glitch).
`coherence test: true` is the one thing that defaults unset `signal`/`glitch` to `wavelets`.
This resolves to one of five run modes:

| `likelihood.components` / `coherence test`         | Run mode     | BayesWave flag(s)        |
|------------------------------------------------------|--------------|---------------------------|
| nothing given                                         | `psd`        | `--cleanOnly` (unchanged) |
| `signal: none`, `glitch: none` (or `components: {}`)  | `psd`        | `--cleanOnly`             |
| `signal: wavelets`, `glitch: none`                    | `signal`     | `--signalOnly`            |
| `signal: none`, `glitch: wavelets`                    | `glitch`     | `--glitchOnly`            |
| `signal: wavelets`, `glitch: wavelets` (no coherence test) | `full`  | `--fullOnly`              |
| `coherence test: true` (any components, or none)      | `coherence`  | *(no restriction flag)*   |

`full` mode (`--fullOnly`) runs BayesWave's *combined* signal+glitch model: it's the right
choice for a joint reconstruction, but BayesWave only writes a single "full" evidence for
it, not separate signal/glitch/noise evidences — there is no `signal:glitch` Bayes factor to
compute from a `full`-mode run. A coherence test instead needs `signal`, `glitch` **and**
`noise` run as genuinely independent phases so their evidences can be compared; that's
BayesWave's own default model set with no restriction flag at all, which is exactly what
`coherence test: true` (`"coherence"` mode) requests. Because of this, `coherence test:
true` combined with `signal: none` or `glitch: none` is rejected with a
`PipelineException` — a coherence test needs both. On-source PSD estimation ("clean"
model) always runs alongside whichever mode is chosen, so
`collect_assets()["psds"]`/`["xml psds"]` keep working the same way regardless of mode.

Once the production completes, `collect_assets()` additionally returns:

- `"reconstructions"`: `{component: {ifo: path}}` for each of `"signal"`/`"glitch"` that was
  requested — the median time-domain waveform reconstruction BayesWavePost produces.
- `"bayes factors"`: log Bayes factors parsed from BayesWave's `evidence.dat`, e.g.
  `{"signal:noise": 12.3, "signal:glitch": 6.1, "glitch:noise": 6.2}` — **only for
  `"coherence"` mode** (BayesWave always writes placeholder `"<model> 0 0"` lines to
  `evidence.dat` for models that didn't actually run, so this is `{}` for every other mode,
  including `full`, rather than risk misleading zero-based Bayes factors).
- `"skymap"`: path to `plots/skymap.png`, produced by megaplot.py once a signal model has run.

`after_completion()` stores the reconstructions to the event repository and the Asimov store
the same way it does PSDs, and merges the Bayes factors into `production.meta`.

## Usage

### Via Asimov CLI

```bash
# Build the DAG
asimov manage build --production Prod0

# Submit the job
asimov manage submit --production Prod0

# Monitor progress
asimov manage monitor
```

### Via Python API

```python
from asimov_bayeswave import BayesWave

# Create pipeline instance
pipeline = BayesWave(production)

# Build and submit
pipeline.build_dag()
pipeline.submit_dag()

# Collect results after completion
assets = pipeline.collect_assets()
psds = assets["psds"]
xml_psds = assets["xml psds"]
reconstructions = assets["reconstructions"]  # {} unless likelihood.components requested them
bayes_factors = assets["bayes factors"]      # {} likewise
```

## Requirements

- Python >= 3.9
- asimov >= 0.7.0
- numpy
- BayesWave (must be installed separately) — via conda-forge:
  ```bash
  conda install -c conda-forge bayeswave bayeswaveutils
  ```
  `bayeswave` ships the compiled samplers (`BayesWave`, `BayesWavePost`, ...);
  `bayeswave_pipe` (the DAG-generation script this plugin's `build_dag()` shells out to)
  and `megaplot.py`/`megasky.py` come from the separate `bayeswaveutils` package. Unlike
  the sibling `asimov-lalinference` plugin, no `numpy<2` pin is needed — the current
  conda-forge `bayeswaveutils` build's `megaplot.py` has already been patched for numpy
  2.0.

## Operational notes

### `convert_psd_ascii2xml` is not available from public conda-forge packages

`after_completion()` tries to convert each ascii-format PSD to XML via a
`convert_psd_ascii2xml` executable. As of this writing that tool is not shipped by any
current public conda-forge package — `bayeswave`, `bayeswaveutils`, `lalinference` and
`lalapps` were all checked while building this plugin's end-to-end test, and none of them
provide it (it may only exist in older or IGWN-internal environments). `bayeswave` does
ship a `BayesWaveToLALPSD` executable that looks like a plausible modern replacement, but
its calling convention (positional run name, requires `--gnuplot` output enabled during
the original run, reads specific paths under `waveforms/`) is substantially different and
has not been validated here.

This is handled gracefully rather than worked around: `after_completion()` catches the
resulting `PipelineException`, logs it, and continues on to store the ascii-format PSD via
`store_assets()` regardless (see `collect_assets()["psds"]`). If your environment does
have a working `convert_psd_ascii2xml`, XML-format PSD conversion and storage will work as
documented above with no changes needed. If not, downstream pipelines that specifically
need an XML-format PSD (rather than the ascii format) won't get one from this plugin until
someone wires up `BayesWaveToLALPSD` (or an equivalent) as a real replacement.

## Documentation

Full documentation is available at [asimov-bayeswave.readthedocs.io](https://asimov-bayeswave.readthedocs.io).

### Building Documentation Locally

```bash
cd docs
make html
```

The built documentation will be in `docs/build/html/`.

## Testing

Run the unit test suite with:

```bash
pytest
```

For coverage reporting:

```bash
pytest --cov=asimov_bayeswave --cov-report=html
```

### End-to-end test

`.github/workflows/e2e.yml` runs a genuine end-to-end test on real GitHub Actions
infrastructure: a real `bayeswave_pipe` DAG (`BayesWave` clean run -> `BayesWavePost` ->
`megaplot.py`), submitted to and run by a real (disposable, in-container) HTCondor pool,
against real (trimmed, ~32s) GW150914 H1 GWOSC strain vendored into
`tests/test_data/frames/`. It waits for and validates a genuine, parseable
`glitch_median_PSD_forLI_H1.dat` — the same file `detect_completion()`/`collect_assets()`
themselves look for — not just "the DAG was submitted", and separately checks that a
production reaches a genuinely finished/uploaded state and that the missing
`convert_psd_ascii2xml` tool (see "Operational notes" above) is handled gracefully. It's
what found several of the real bugs described in `CHANGELOG.md`.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure:
- All tests pass
- New features include tests
- Documentation is updated
- Code follows PEP 8 style guidelines

## Migration from Asimov 0.6

If you're upgrading from Asimov 0.6 which included BayesWave support natively:

1. Install this plugin: `pip install asimov-bayeswave`
2. The plugin will be automatically discovered by Asimov 0.7+
3. No changes to your configuration files are required

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Authors

- Daniel Williams (daniel.williams@ligo.org)

## Acknowledgments

- The LIGO Scientific Collaboration
- The BayesWave development team
- The Asimov development team

## Citation

If you use this software in your research, please cite:

```bibtex
@software{asimov_bayeswave,
  author = {Williams, Daniel},
  title = {asimov-bayeswave: BayesWave integration for Asimov},
  url = {https://github.com/transientlunatic/asimov-bayeswave},
  year = {2026}
}
```

## Support

For issues, questions, or contributions, please use the [GitHub issue tracker](https://github.com/transientlunatic/asimov-bayeswave/issues).
