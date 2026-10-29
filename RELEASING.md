# Release checklist

Bloom v0.0.4 targets DuckDB **v2.0.0**, pinned at
`d54cbbe3df17e8e5ce68312f483b4155836112aa`. Release-candidate workflows produce
downloadable CI artifacts; community publication and immutable release tags
follow the gates below.

## Before a release

1. Keep the `duckdb` submodule, distribution workflow's `duckdb_commit`, and
   release-candidate default fixed to the full commit above. Keep the
   `duckdb_version`, Makefile's `OVERRIDE_GIT_DESCRIBE`, and direct CMake builds'
   `DUCKDB_EXPLICIT_VERSION` at `v2.0.0`. DuckDB 2.0 does not infer its release
   version from Git tags. Pin `extension-ci-tools` and reusable workflows to
   the same full tools commit.
2. Update `EXTENSION_VERSION` in `extension_config.cmake`, the README version
   badge, and `community-extension-description.yml.template`.
3. Build with `make release -j"$(nproc)"` and run:

   ```bash
   make format-check
   make tidy-check
   build/release/test/unittest 'test/sql/*'
   python3 -m unittest discover --start-directory test/python
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

## Publish for DuckDB 2.0.0

1. Complete every gate above against the pinned stable commit. The packaged
   custom repository uses `v2.0.0` as its DuckDB revision.
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
   SELECT current_setting('enable_bloom');
   ```

For an update, bump the extension version and submit a new community PR that
updates both `extension.version` and `repo.ref`. Pushing Bloom's `main` alone
does not update the community artifact. Existing installations can download
the published update with `FORCE INSTALL bloom FROM community;`; restart the
DuckDB process before loading it. Check the installed version and repository:

```sql
SELECT extension_version, installed_from
FROM duckdb_extensions() WHERE extension_name = 'bloom';
```

For a future DuckDB version that requires incompatible source changes, use a
`vX.Y-<codename>` compatibility branch and the community descriptor's
`repo.ref_next` field before the DuckDB release.
