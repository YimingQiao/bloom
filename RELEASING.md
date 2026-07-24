# Release checklist

Bloom targets its first supported release with DuckDB 1.6. Until that version
is stable, `main` tracks a pinned DuckDB commit and release-candidate workflows
only produce downloadable CI artifacts; they do not publish a GitHub release.

## Before a release

1. Update the `duckdb` submodule and the identical full commit in
   `.github/workflows/MainDistributionPipeline.yml` and
   `.github/workflows/ReleaseCandidate.yml`.
2. Update `EXTENSION_VERSION` in `extension_config.cmake`, the README version
   badge, and `community-extension-description.yml.template`.
3. Build with `make release -j48` and run:

   ```bash
   make format-check
   build/release/test/unittest 'test/sql/bloom.test'
   ```

4. Run both `DuckDB Full Compatibility` configurations. RPT-disabled results
   must match the same pinned DuckDB build; RPT-enabled results must add no
   extension-specific failures.
5. Run the one-thread IMDb and TPC-H SF10 regression gate. One warmup and two
   timed runs are enough for the pre-release check:

   ```bash
   python3 scripts/run_benchmark.py --workload imdb --timed-runs 2
   python3 scripts/run_benchmark.py --workload tpch_sf10 --timed-runs 2
   ```

6. Run `Release Candidate` manually. It builds the supported native platform
   matrix and packages deterministic gzip files, checksums, a manifest, and a
   DuckDB custom-repository tree. Its DuckDB input must match the committed
   submodule exactly.
7. Test at least one packaged artifact with the matching DuckDB binary using
   `-unsigned`, then run the Bloom SQL tests against the packaged extension.

## DuckDB 1.6 publication

1. Replace the pinned development commit with the commit behind the final
   `v1.6.x` tag and repeat every gate above. The packaging workflow detects the
   stable tag and uses it as DuckDB's repository revision.
2. Copy `community-extension-description.yml.template` to
   `extensions/bloom/description.yml` in
   [duckdb/community-extensions](https://github.com/duckdb/community-extensions),
   replace `<release-commit>` with an immutable Bloom commit, and submit it.
3. Create an immutable Bloom tag only after CI is green. Do not move an
   existing tag.
4. After the community build passes, verify from an official DuckDB 1.6 binary:

   ```sql
   INSTALL bloom FROM community;
   LOAD bloom;
   SELECT current_setting('enable_rpt');
   ```

For a future DuckDB version that requires incompatible source changes, use a
`vX.Y-<codename>` compatibility branch and the community descriptor's
`repo.ref_next` field before the DuckDB release.
