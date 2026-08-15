# Experiment archive

This directory keeps Bloom's curated research evidence separate from the
extension's user-facing benchmark suite. Exploratory studies and raw runs stay
local until they have a stable protocol and a validated result.

## Current results

- [Full prepared/instant benchmark](2026-08-prepared-instant-full-benchmark/README.md):
  the same-engine comparison across all eight Bloom workloads, including warm
  and cold storage states and per-query correctness validation.
- [Oracle, prepared, and instant sampling](2026-08-oracle-prepared-instant-end-to-end/README.md):
  the focused JOB, TPC-H SF10, and Appian comparison where a complete oracle
  cardinality cache is available.

## Artifact policy

Every benchmark or research experiment that supports a Bloom design decision
must live in its own dated directory. An experiment is not complete until the
directory contains:

1. the question and hypothesis;
2. exact source, DuckDB, workload, database, and machine fingerprints;
3. one-command reproduction scripts and all non-default settings;
4. compressed raw logs, parsed records, summaries, and artifact checksums;
5. correctness checks and known limitations;
6. observations separated from the final conclusion.

Large input databases do not need to be committed, but their byte size and
SHA-256 digest must be recorded. Raw logs, parsed JSONL, sample caches, and
`reproduced/` runs remain in the local experiment directory but are ignored by
Git; curated reports, summaries, scripts, settings, and artifact manifests are
the reviewable repository surface. Publish a separately checksummed artifact
bundle when raw evidence needs to accompany a paper or release. Never
overwrite archived results when reproducing an experiment: write new output
under `reproduced/`.

The full prepared/instant directory is the reference layout for new results.
