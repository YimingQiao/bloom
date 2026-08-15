# Release checklist

Bloom will provide formal release support starting with DuckDB's next stable
release. Until then, `main` tracks a pinned DuckDB commit and release-candidate
workflows only produce downloadable CI artifacts; they do not publish a GitHub
release.

## Before a release

1. Update the `duckdb` submodule and the identical full commit in
   `.github/workflows/MainDistributionPipeline.yml` and
   `.github/workflows/ReleaseCandidate.yml`.
2. Update `EXTENSION_VERSION` in `extension_config.cmake`, the README version
   badge, and `community-extension-description.yml.template`.
3. Build with `make release -j"$(nproc)"` and run:

   ```bash
   make format-check
   make tidy-check
   build/release/test/unittest 'test/sql/*'
   ```

4. Run both `DuckDB Full Compatibility` configurations. RPT-disabled results
   must match the same pinned DuckDB build; RPT-enabled results must add no
   extension-specific failures.
5. Run the one-thread IMDb and TPC-H SF10 regression gate with DuckDB's native
   benchmark runner. This is the protocol used by the main README table: one
   discarded warmup in the same process followed by five timed runs. Do not
   compare those absolute totals with the fresh-process prepared/instant table,
   whose `warm` state means OS page residency rather than a warmed DuckDB buffer
   manager.

   ```bash
   python3 scripts/run_benchmark_suite.py \
     --workload imdb --threads 1 --timed-runs 5 \
     --sampling-mode prepared --sample-seed 2
   python3 scripts/run_benchmark_suite.py \
     --workload tpch_sf10 --threads 1 --timed-runs 5 \
     --sampling-mode prepared --sample-seed 2
   ```

6. Run `Release Candidate` manually. It builds the supported native platform
   matrix and packages deterministic gzip files, checksums, a manifest, and a
   DuckDB custom-repository tree. Its DuckDB input must match the committed
   submodule exactly.
7. Test at least one packaged artifact with the matching DuckDB binary using
   `-unsigned`, then run the Bloom SQL tests against the packaged extension.

## Next stable DuckDB release

1. Replace the pinned development commit with the commit behind the final
   stable tag and repeat every gate above. The packaging workflow detects the
   tag and uses it as DuckDB's repository revision.
2. Copy `community-extension-description.yml.template` to
   `extensions/bloom/description.yml` in
   [duckdb/community-extensions](https://github.com/duckdb/community-extensions),
   replace `<release-commit>` with an immutable Bloom commit, and submit it.
3. Create an immutable Bloom tag only after CI is green. Do not move an
   existing tag.
4. After the community build passes, verify from an official binary of that
   DuckDB release:

   ```sql
   INSTALL bloom FROM community;
   LOAD bloom;
   SELECT current_setting('enable_rpt');
   ```

For a future DuckDB version that requires incompatible source changes, use a
`vX.Y-<codename>` compatibility branch and the community descriptor's
`repo.ref_next` field before the DuckDB release.
