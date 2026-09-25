Usage
=====

Basic Usage
-----------

Once ``asimov-bayeswave`` is installed, it will be automatically available
in Asimov. You can specify it as the pipeline in your production configuration:

.. code-block:: yaml

   name: GW150914
   productions:
   - Prod0:
       pipeline: bayeswave
       comment: PSD generation with BayesWave
       status: wait
       meta:
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

Configuration
-------------

BayesWave jobs are configured through the production metadata. Key configuration
sections include:

Likelihood Settings
~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   likelihood:
     sample rate: 2048          # Sampling rate in Hz
     segment length: 8          # Segment length in seconds
     window length: 4           # Window length (optional)
     psd length: 8              # PSD estimation length
     roll off time: 1.0         # Roll-off time
     iterations: 100000         # Number of MCMC iterations
     chains: 8                  # Number of parallel chains
     threads: 4                 # Threads per chain

Signal/glitch component selection (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default no ``likelihood.components`` is given, and a production runs BayesWave in
its original PSD-only ("cleanOnly") mode, exactly as before this option existed. To also
run BayesWave's signal and/or glitch wavelet models -- for source reconstructions, a
coherence test, or both -- add a ``components`` block under ``likelihood``:

.. code-block:: yaml

   likelihood:
     components:
       signal: wavelets   # none | wavelets | chirplets | cbc (cbc: not yet supported)
       glitch: wavelets   # none | wavelets | chirplets
       noise:
         psd: fit         # fit | fixed (fixed: not yet supported)
         lines: true       # BayesLine spectral-line modelling (default: true)
     coherence test: true  # implies signal: wavelets, glitch: wavelets if not given

``signal``/``glitch`` set to ``wavelets`` or ``chirplets`` enable that model; ``none``
disables it. Setting only one of them runs BayesWave in signal-only or glitch-only mode;
setting both (or setting ``coherence test: true`` on its own, with no ``components`` block
at all) runs the combined signal+glitch ("full") model used for a coherence test. On-source
PSD estimation always runs alongside whichever of these is requested, so
``collect_assets()["psds"]``/``["xml psds"]`` are unaffected by ``components``.

Unsupported combinations (``signal: cbc``, ``noise: {psd: fixed}``) or unknown values raise
a clear :class:`~asimov.pipeline.PipelineException` from ``build_dag()`` rather than being
silently ignored or mis-run.

Data Settings
~~~~~~~~~~~~~

.. code-block:: yaml

   data:
     channels:
       H1: H1:GDS-CALIB_STRAIN
       L1: L1:GDS-CALIB_STRAIN
     frame types:
       H1: H1_HOFT_C00
       L1: L1_HOFT_C00
     cache files:               # Optional: use pre-generated cache files
       H1: /path/to/H1.cache
       L1: /path/to/L1.cache
     segment length: 8

Quality Settings
~~~~~~~~~~~~~~~~

.. code-block:: yaml

   quality:
     minimum frequency:
       H1: 20
       L1: 20
     lowest minimum frequency: 20  # Auto-calculated if not provided
     supress:                      # Optional: suppress frequency bands
       H1:
         lower: 60
         upper: 60.5

Scheduler Settings
~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   scheduler:
     accounting group: ligo.dev.o4.cbc.pe.bayeswave
     request memory: 8192 MB
     request post memory: 16384 MB
     request disk: 64 MB
     request post disk: 64 MB
     copy frames: false          # Copy frame files to run directory
     osg: false                  # Use OSG resources

Working with PSDs
-----------------

BayesWave produces power spectral density (PSD) estimates that can be used
by downstream parameter estimation pipelines. The plugin automatically:

1. Collects PSDs from the BayesWave output
2. Converts them to XML format for use with LALInference
3. Stores them in the Asimov storage system
4. Commits them to the event repository

Accessing PSDs Programmatically
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from asimov_bayeswave import BayesWave

   # After job completion
   pipeline = BayesWave(production)
   assets = pipeline.collect_assets()

   # Access ASCII PSDs
   for ifo, psd_path in assets["psds"].items():
       print(f"{ifo}: {psd_path}")

   # Access XML PSDs
   for ifo, psd_path in assets["xml psds"].items():
       print(f"{ifo}: {psd_path}")

Working with Reconstructions and Bayes Factors
------------------------------------------------

When ``likelihood.components`` requests a signal and/or glitch run (see above),
``collect_assets()`` additionally returns waveform reconstructions, log Bayes factors and
(for a signal run) a sky map once the production has completed:

.. code-block:: python

   from asimov_bayeswave import BayesWave

   pipeline = BayesWave(production)
   assets = pipeline.collect_assets()

   # {"signal": {"H1": "...dat", "L1": "...dat"}, "glitch": {...}}
   for component, per_ifo in assets["reconstructions"].items():
       for ifo, path in per_ifo.items():
           print(f"{component} reconstruction for {ifo}: {path}")

   # {"signal:noise": 12.3, "signal:glitch": 6.1, "glitch:noise": 6.2}
   print(assets["bayes factors"])

   if "skymap" in assets:
       print(f"Sky map: {assets['skymap']}")

``after_completion()`` stores the reconstruction files to the event repository and the
Asimov store the same way it stores PSDs (see ``store_reconstructions()``), and merges the
Bayes factors into ``production.meta`` (the same way it already does for PSDs).

PSD Suppression
~~~~~~~~~~~~~~~

You can suppress specific frequency bands in the PSD (e.g., to remove
instrumental lines):

.. code-block:: python

   pipeline = BayesWave(production)
   pipeline.supress_psd(ifo="H1", fmin=60.0, fmax=60.5)

This sets the PSD to 1.0 in the specified frequency range.

Advanced Features
-----------------

Job Resurrection
~~~~~~~~~~~~~~~~

If a BayesWave job fails, it can be automatically resurrected using
HTCondor rescue DAGs:

.. code-block:: python

   pipeline = BayesWave(production)
   pipeline.resurrect()

This will resubmit the job using any available rescue files, up to
a maximum of 5 attempts.

HTML Output
~~~~~~~~~~~

The plugin automatically collects megaplot output for visualization:

.. code-block:: python

   pipeline = BayesWave(production)
   html_output = pipeline.html()

This generates HTML links to the megaplot results page. When ``likelihood.components``
requested a signal and/or glitch run, the HTML also includes any reconstruction plots, a
log Bayes-factor table and a sky map, when those assets are present in ``production.meta``.

Completion detection with signal/glitch components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a PSD-only production, ``detect_completion()`` behaves exactly as before: it reports
completion once the on-source PSDs exist. For a production which also requested a signal
and/or glitch model, PSD estimation ("clean" model) finishes well before the rest of
post-processing does, so ``detect_completion()`` instead requires the reconstruction for
every requested model and interferometer, plus the final megaplot summary page, before
reporting completion.

Command Line Usage
------------------

If you're using Asimov's command-line interface:

.. code-block:: bash

   # Build the DAG
   asimov manage build --production Prod0

   # Submit the job
   asimov manage submit --production Prod0

   # Check status
   asimov manage monitor

The BayesWave plugin will be used automatically based on your
production configuration.
