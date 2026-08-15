# Bloomsea development rules

- Do not add `try`/`catch` blocks to extension-owned C++ under `src/`. Runtime,
  I/O, deserialization, and worker errors must propagate to the caller instead
  of being hidden or converted into a silent fallback.
- Use `D_ASSERT` for internal invariants. Invalid user input and failures that
  can occur in a valid release build still require an explicit DuckDB exception.
- Use DuckDB's `Executor` and `TaskExecutor` directly. On the normal path,
  explicitly call `CancelTasks()` or `WorkOnTasks()` before either the executor
  or state borrowed by its tasks is destroyed; do not add scoped executor
  wrappers solely for exceptional-path cleanup.
- Keep every project header under `src/include/`.
- Do not introduce a new class, result wrapper, enum, or source file unless it
  owns a distinct responsibility that cannot remain a local implementation detail.
- Treat a very large source file as a signal to review its responsibilities.
  First split the owning class by state lifecycle and invariant; moving one
  unchanged class across several `.cpp` files is not an architectural split.
  Keep a thin coordinator only when the extracted components own real state,
  and group multiple implementation files in a same-named module directory.
- Treat instant sampling as one module. Keep shared scheduling/selection logic
  in `instant_sampler/common.*` and backend-specific code in `native.cpp` or
  `parquet.cpp`; avoid duplicating backend branches in the estimator.
- Keep prepared-sample acquisition, persistence, and preload behavior in
  `prepared_sampler.*`; the sampling estimator only selects a mode and evaluates
  predicates against the resulting sample.
