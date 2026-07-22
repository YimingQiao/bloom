# BloomSea 🌊🌸

A DuckDB extension for **robust predicate transfer**: propagate Bloom / bitmap
filters across joins to prune intermediate results *before* the joins run — a sea
of Bloom filters blossoming across the join graph to find the few rows hidden in an
ocean of data.

The name is an English rendering of 藏海花 ("a rare flower that blooms hidden in
the sea").

## Design

- **Target:** stock **DuckDB v1.5.4** (latest stable) — installs and loads on an
  unmodified DuckDB, no patched build required.
- **Approach:** filters are applied via the **expression-callback** path (filters
  lowered to `Expression`s evaluated by DuckDB's ExpressionExecutor), so BloomSea
  needs no changes to DuckDB internals. (The research prototype used a faster
  native-table-filter path that required patching DuckDB; BloomSea trades some
  per-filter speed for zero-patch portability.)
- Registers an **optimizer extension** that rewrites the logical plan to build and
  push predicate-transfer filters.

Ported from the research prototype (native predicate transfer via adaptive
excitation, with oracle and sampling cardinality estimators).

## Status — scaffolding

- [x] Extension-template skeleton (`bloomsea_extension.cpp`, CMake, Makefile, test)
- [ ] Pin `duckdb` submodule to **v1.5.4**, first build
- [ ] Replace the scalar-function stub with the optimizer-extension registration
- [ ] Port the predicate-transfer optimizer (excitation graph, filter build/push)
- [ ] Bloom + bitmap filters, expression-callback pushdown
- [ ] Sampling cardinality estimator (no precomputed stats)
- [ ] Settings / knobs, docs, tests

## Build

```
git submodule update --init --recursive   # pulls duckdb v1.5.4 + extension-ci-tools
make release                               # or: GEN=ninja make release
```

Loadable extension lands at `build/release/extension/bloomsea/bloomsea.duckdb_extension`.
