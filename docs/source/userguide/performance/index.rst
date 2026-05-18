###########
Performance
###########

Autosubmit ships with built-in support for **CPMIP** (Coupled Model Intercomparison
Project) performance metrics, a community-standard set of indicators originally
proposed for climate models. The metrics give a quick, comparable read on how
efficiently a simulation runs on a given platform, making them a useful first
signal when looking for inefficiencies — undersized partitions, oversubscribed
nodes, I/O bottlenecks, or regressions after a code change.

CPMIP metrics target the **simulation job** of an experiment — the job that
advances model time chunk by chunk (commonly named ``SIM``). SYPD and CHSY are
defined in terms of simulated years, so they rely on the chunk calendar
(``CHUNK``, ``CHUNKSIZE``, ``CHUNKSIZEUNIT``) that only chunk-based simulation
jobs carry; CORE_HOURS can still be computed on any job.

CPMIP metrics are not computed automatically: today they are evaluated only
as part of the notification feature. Autosubmit computes a job's CPMIP
metrics when, and only when, both conditions hold:

* ``MAIL.NOTIFICATIONS: True`` is set in the experiment configuration, and
* the job declares a ``CPMIP_THRESHOLDS`` block in ``jobs_<EXPID>.yml``.

Without that, the metric values are not recorded anywhere — there is no
silent computation in the background. See
:ref:`cpmip-notifications-config` for how to enable both.

Available CPMIP metrics
=======================

.. list-table::
   :header-rows: 1
   :widths: 18 50 32

   * - Metric
     - Definition
     - Formula
   * - **SYPD**
     - Simulated Years Per Day — how much model time the job advances per
       wall-clock day. Higher is better.
     - ``simulated_years * 24 / runtime_hours``
   * - **CHSY**
     - Core-Hours per Simulated Year — total CPU time consumed per year of
       simulation. Lower is better.
     - ``ncpus * runtime_hours / simulated_years``
   * - **CORE_HOURS**
     - Total core-hours billed by the run. Useful as a budget signal.
     - ``ncpus * runtime_hours``

Inputs come from the job stat file (``start_time``, ``end_time``) and the
experiment configuration (``PROCESSORS`` plus the chunk calendar). If a required
input is missing, the metric is silently skipped — for example, ``CORE_HOURS``
and ``CHSY`` are skipped when ``PROCESSORS`` is not set on the job.

Detecting inefficiencies
========================

The three metrics answer complementary questions:

* **Low SYPD** → the job is slow relative to wall-clock. Useful for catching
  regressions after a code change, a new compiler, or a different
  platform/partition. Compare against a known-good SYPD baseline for the same
  model on the same platform.
* **High CHSY** → the job is *expensive per simulated year*. A run can have an
  acceptable SYPD but still burn excessive core-hours if scaled past the
  strong-scaling sweet spot. CHSY is the canonical metric for catching that.
* **High CORE_HOURS** → the job is consuming more allocation than expected.
  Useful as a budget guard against runaway runs (e.g., a chunk that should
  take 1 000 core-hours but takes 10 000 because of a wait/retry loop).
