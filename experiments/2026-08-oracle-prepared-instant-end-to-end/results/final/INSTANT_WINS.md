# Why instant sampling is faster on individual queries

This report explains every query whose final median is lower with instant sampling. It does **not** assume that a lower median means a better estimate. End-to-end time, repeat separation, transfer signatures, RPT phase timing, isolated sampling latency, and cold physical input are reported separately.

## Reading the evidence

- `Repeat-separated=yes` means every measured Instant repetition was faster than every Prepared repetition (two repetitions warm, three cold). It is stronger than a median difference, but is not a confidence interval.
- `RPT gain` and `materialize gain` are state-matched diagnostic Prepared-minus-Instant phase times. Positive is favorable to Instant. These logs are independent of the logging-disabled final timer, so use them as mechanism evidence rather than add them to the final delta.
- Cold `I/O ratio` includes sampling and final execution. A value below one means Instant read fewer physical bytes even after paying for sampling.
- An exact execution-signature match rules out a better transfer plan. Any remaining stable gain comes from the direct sampler changing buffer/decode/prefetch state, or from runtime variation.

## Overall classification

There are 84 warm wins and 74 cold wins. 95 have non-overlapping repetition ranges; 63 remain noise-compatible.

The recurring mechanisms are:

1. **Different excitation timing.** The unique directed transfer relationships usually remain the same, but estimates change which source fires first and whether an edge fires again. Stronger filters can therefore exist before an expensive materialization.
2. **Base-block preconditioning.** Prepared samples are separate in-memory CDCs and do not touch base-table blocks. Instant sampling reads base storage through DuckDB's buffer manager; the same blocks, compression metadata, and decoded paths can be reused by materialization and final scans. This applies even when the OS page cache is warm because every timed query uses a fresh DuckDB process/buffer manager.
3. **Cold asynchronous prefetch and overlap.** The disk sampler resolves codec-aware block handles and prefetches them in parallel. Sampling I/O is not necessarily additive when the formal scan later needs those blocks.
4. **Noise.** Small median wins with overlapping repetitions are observations, not evidence that Instant is intrinsically faster.

## Warm wins

### JOB (59 queries)

| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | Evidence-based explanation |
|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---|
| 02a | 86.0 ms | 80.0 ms | 0.930x | 6.0 ms | 10.18 ms | yes | 边/次数不变，最终顺序变化 | +5.1 ms | +21.6 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 02b | 84.0 ms | 76.0 ms | 0.905x | 8.0 ms | 9.09 ms | yes | 完整 excitation 相同 | -18.2 ms | -0.7 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 02d | 101.0 ms | 94.0 ms | 0.931x | 7.0 ms | 3.52 ms | yes | 完整 excitation 相同 | -6.4 ms | -1.0 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 03a | 354.5 ms | 310.5 ms | 0.876x | 44.0 ms | 9.97 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +10.5 ms | +27.5 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 03b | 484.0 ms | 474.0 ms | 0.979x | 10.0 ms | 4.51 ms | yes | 完整 excitation 相同 | -0.1 ms | +7.4 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| 03c | 412.5 ms | 401.5 ms | 0.973x | 11.0 ms | 4.31 ms | no | 边/次数不变，最终顺序变化 | -11.1 ms | -6.3 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 04a | 87.0 ms | 82.0 ms | 0.943x | 5.0 ms | 5.20 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -4.2 ms | +2.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 04c | 97.5 ms | 90.0 ms | 0.923x | 7.5 ms | 4.44 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -8.8 ms | -3.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 05c | 282.5 ms | 269.5 ms | 0.954x | 13.0 ms | 5.21 ms | no | 边/次数不变，最终顺序变化 | -5.8 ms | -0.3 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 06b | 247.5 ms | 244.5 ms | 0.988x | 3.0 ms | 4.64 ms | yes | 完整 excitation 相同 | -6.2 ms | -0.3 ms | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 06c | 249.5 ms | 241.0 ms | 0.966x | 8.5 ms | 4.11 ms | yes | 完整 excitation 相同 | -24.8 ms | -18.9 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 06d | 261.0 ms | 259.0 ms | 0.992x | 2.0 ms | 3.98 ms | no | 完整 excitation 相同 | -12.3 ms | -6.1 ms | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 06f | 487.5 ms | 415.5 ms | 0.852x | 72.0 ms | 4.20 ms | yes | 边不变，执行次数变化 | +24.9 ms | +27.9 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 07c | 679.5 ms | 626.5 ms | 0.922x | 53.0 ms | 15.94 ms | yes | 边/次数不变，最终顺序变化 | -7.6 ms | +16.4 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 09a | 336.5 ms | 320.5 ms | 0.952x | 16.0 ms | 16.99 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -17.2 ms | +3.5 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 09c | 433.5 ms | 414.0 ms | 0.955x | 19.5 ms | 7.04 ms | yes | 边不变，执行次数变化 | -13.3 ms | -3.2 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 09d | 643.0 ms | 628.0 ms | 0.977x | 15.0 ms | 7.62 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -6.7 ms | +1.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 10c | 1009.5 ms | 937.0 ms | 0.928x | 72.5 ms | 5.18 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +20.5 ms | +25.7 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 12a | 113.5 ms | 111.5 ms | 0.982x | 2.0 ms | 6.52 ms | no | 边不变，执行次数变化 | -1.1 ms | +7.3 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 14c | 124.0 ms | 123.5 ms | 0.996x | 0.5 ms | 6.29 ms | no | 边/次数不变，最终顺序变化 | -7.8 ms | +0.4 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 15c | 264.5 ms | 244.5 ms | 0.924x | 20.0 ms | 6.89 ms | no | 边不变，执行次数变化 | -56.9 ms | -48.8 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 15d | 182.0 ms | 178.5 ms | 0.981x | 3.5 ms | 5.52 ms | yes | 边不变，执行次数变化 | -7.3 ms | +0.6 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 16a | 328.5 ms | 279.0 ms | 0.849x | 49.5 ms | 5.76 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -11.8 ms | -4.4 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 16b | 1104.5 ms | 1088.5 ms | 0.986x | 16.0 ms | 5.01 ms | no | 边不变，执行次数变化 | -56.8 ms | -42.9 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 16c | 415.5 ms | 372.0 ms | 0.895x | 43.5 ms | 5.26 ms | yes | 边不变，执行次数变化 | +2.1 ms | +7.2 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 16d | 401.5 ms | 357.0 ms | 0.889x | 44.5 ms | 4.78 ms | yes | 边不变，执行次数变化 | -6.8 ms | +1.5 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 17a | 234.0 ms | 232.0 ms | 0.991x | 2.0 ms | 4.33 ms | yes | 完整 excitation 相同 | -5.1 ms | +1.1 ms | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 17b | 232.5 ms | 220.5 ms | 0.948x | 12.0 ms | 3.53 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -6.8 ms | -0.8 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 17c | 213.5 ms | 200.0 ms | 0.937x | 13.5 ms | 4.38 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -8.8 ms | -1.2 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 17d | 414.5 ms | 364.0 ms | 0.878x | 50.5 ms | 3.48 ms | yes | 边不变，执行次数变化 | -9.0 ms | -4.1 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 17e | 618.5 ms | 557.0 ms | 0.901x | 61.5 ms | 3.90 ms | yes | 边不变，执行次数变化 | -1.7 ms | +5.9 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 17f | 650.0 ms | 588.5 ms | 0.905x | 61.5 ms | 3.82 ms | yes | 完整 excitation 相同 | -9.3 ms | -2.7 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 18a | 484.0 ms | 448.0 ms | 0.926x | 36.0 ms | 8.79 ms | yes | 边/次数不变，最终顺序变化 | +5.3 ms | +15.9 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 18b | 513.5 ms | 453.5 ms | 0.883x | 60.0 ms | 10.88 ms | yes | 完整 excitation 相同 | -12.7 ms | +0.7 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 18c | 789.5 ms | 733.0 ms | 0.928x | 56.5 ms | 8.46 ms | yes | 边不变，执行次数变化 | -3.4 ms | +7.4 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 19a | 280.0 ms | 266.0 ms | 0.950x | 14.0 ms | 11.15 ms | no | 完整 excitation 相同 | -15.7 ms | -0.8 ms | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 19c | 453.5 ms | 380.5 ms | 0.839x | 73.0 ms | 9.19 ms | yes | 边不变，执行次数变化 | +5.8 ms | +17.3 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 19d | 664.5 ms | 657.0 ms | 0.989x | 7.5 ms | 7.11 ms | no | 边不变，执行次数变化 | -16.9 ms | -7.6 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 20a | 649.5 ms | 587.5 ms | 0.905x | 62.0 ms | 5.38 ms | yes | 边/次数不变，最终顺序变化 | -1.2 ms | +8.4 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 20b | 461.5 ms | 456.5 ms | 0.989x | 5.0 ms | 5.67 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -6.9 ms | +0.9 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 20c | 709.0 ms | 633.5 ms | 0.894x | 75.5 ms | 5.39 ms | yes | 完整 excitation 相同 | -8.8 ms | -1.8 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 22a | 189.5 ms | 189.0 ms | 0.997x | 0.5 ms | 9.79 ms | no | 边不变，执行次数变化 | -13.7 ms | -0.7 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 22c | 202.5 ms | 201.5 ms | 0.995x | 1.0 ms | 9.72 ms | no | 边不变，执行次数变化 | -11.8 ms | +1.2 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 22d | 171.0 ms | 166.0 ms | 0.971x | 5.0 ms | 7.96 ms | yes | 边不变，执行次数变化 | -7.6 ms | +1.4 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 23b | 226.5 ms | 126.0 ms | 0.556x | 100.5 ms | 8.47 ms | yes | 边不变，执行次数变化 | +105.9 ms | +116.2 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 24a | 402.5 ms | 381.5 ms | 0.948x | 21.0 ms | 12.38 ms | yes | 边不变，执行次数变化 | -5.2 ms | +6.9 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 25a | 735.0 ms | 688.0 ms | 0.936x | 47.0 ms | 10.58 ms | yes | 边不变，执行次数变化 | -1.1 ms | +9.9 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 25b | 338.0 ms | 329.5 ms | 0.975x | 8.5 ms | 10.01 ms | yes | 完整 excitation 相同 | -14.8 ms | -2.5 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 25c | 805.0 ms | 731.0 ms | 0.908x | 74.0 ms | 10.62 ms | yes | 边不变，执行次数变化 | +9.9 ms | +22.0 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 26a | 703.0 ms | 634.5 ms | 0.903x | 68.5 ms | 8.35 ms | yes | 边/次数不变，最终顺序变化 | -12.0 ms | -1.9 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 26b | 602.0 ms | 543.5 ms | 0.903x | 58.5 ms | 7.53 ms | yes | 完整 excitation 相同 | -13.6 ms | -3.7 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 26c | 682.0 ms | 621.0 ms | 0.911x | 61.0 ms | 8.50 ms | yes | 边不变，执行次数变化 | -13.7 ms | -3.1 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 29a | 408.5 ms | 368.5 ms | 0.902x | 40.0 ms | 12.67 ms | no | 完整 excitation 相同 | -23.3 ms | -6.7 ms | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 29c | 348.0 ms | 345.5 ms | 0.993x | 2.5 ms | 13.11 ms | no | 边不变，执行次数变化 | -17.7 ms | 0.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 30a | 590.5 ms | 530.0 ms | 0.898x | 60.5 ms | 11.74 ms | yes | 边不变，执行次数变化 | -0.8 ms | +11.3 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 30b | 424.5 ms | 403.5 ms | 0.951x | 21.0 ms | 11.01 ms | yes | 完整 excitation 相同 | -11.7 ms | +2.0 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| 30c | 757.5 ms | 686.0 ms | 0.906x | 71.5 ms | 12.28 ms | yes | 边不变，执行次数变化 | +9.3 ms | +23.0 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| 31a | 557.5 ms | 527.0 ms | 0.945x | 30.5 ms | 14.41 ms | yes | 边/次数不变，最终顺序变化 | -16.7 ms | -4.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |
| 31c | 590.5 ms | 548.0 ms | 0.928x | 42.5 ms | 11.65 ms | yes | 边不变，执行次数变化 | -22.3 ms | -10.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |

### TPC-H SF10 (19 queries)

| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | Evidence-based explanation |
|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---|
| q01 | 1455.5 ms | 1439.5 ms | 0.989x | 16.0 ms | 0.00 ms | yes | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q03 | 752.0 ms | 629.5 ms | 0.837x | 122.5 ms | 26.53 ms | yes | 完整 excitation 相同 | +432.4 ms | +468.5 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q04 | 802.5 ms | 795.0 ms | 0.991x | 7.5 ms | 0.00 ms | no | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q05 | 931.0 ms | 757.5 ms | 0.814x | 173.5 ms | 7.53 ms | yes | 完整 excitation 相同 | +232.2 ms | +246.1 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q07 | 869.5 ms | 730.0 ms | 0.840x | 139.5 ms | 2.68 ms | yes | 完整 excitation 相同 | +25.5 ms | +24.1 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q08 | 987.0 ms | 791.0 ms | 0.801x | 196.0 ms | 8.03 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +103.5 ms | +115.8 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| q09 | 2064.5 ms | 1941.0 ms | 0.940x | 123.5 ms | 10.32 ms | yes | 边不变，执行次数变化 | -16.6 ms | +50.6 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| q10 | 1604.5 ms | 1409.0 ms | 0.878x | 195.5 ms | 16.49 ms | yes | 完整 excitation 相同 | +73.8 ms | +87.8 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q11 | 146.5 ms | 129.5 ms | 0.884x | 17.0 ms | 3.63 ms | yes | 完整 excitation 相同 | +7.8 ms | +11.6 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q12 | 1012.0 ms | 870.5 ms | 0.860x | 141.5 ms | 19.11 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +10.6 ms | +13.5 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低 |
| q13 | 2559.0 ms | 2469.0 ms | 0.965x | 90.0 ms | 8.08 ms | yes | 完整 excitation 相同 | -10.7 ms | 0.0 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| q14 | 731.0 ms | 636.5 ms | 0.871x | 94.5 ms | 2.19 ms | yes | 完整 excitation 相同 | +1.9 ms | +5.7 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q15 | 633.5 ms | 628.5 ms | 0.992x | 5.0 ms | 0.00 ms | no | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q16 | 408.0 ms | 400.0 ms | 0.980x | 8.0 ms | 3.03 ms | yes | 完整 excitation 相同 | +2.5 ms | +5.5 ms | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q18 | 2653.0 ms | 2580.0 ms | 0.972x | 73.0 ms | 7.56 ms | yes | 完整 excitation 相同 | -7.5 ms | 0.0 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| q19 | 1154.5 ms | 1122.5 ms | 0.972x | 32.0 ms | 0.00 ms | yes | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q20 | 1061.0 ms | 957.0 ms | 0.902x | 104.0 ms | 3.28 ms | yes | 完整 excitation 相同 | +0.7 ms | +6.7 ms | 计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持 |
| q21 | 1731.0 ms | 1629.5 ms | 0.941x | 101.5 ms | 4.55 ms | yes | 完整 excitation 相同 | -12.0 ms | -8.4 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| q22 | 428.5 ms | 419.5 ms | 0.979x | 9.0 ms | 0.00 ms | yes | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |

### Appian (6 queries)

| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | Evidence-based explanation |
|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---|
| q01 | 1292.5 ms | 1239.5 ms | 0.959x | 53.0 ms | 11.45 ms | yes | 完整 excitation 相同 | -2.8 ms | 0.0 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| q02 | 2941.0 ms | 2906.0 ms | 0.988x | 35.0 ms | 39.56 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -2.5 ms | 0.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q03 | 2509.0 ms | 2466.5 ms | 0.983x | 42.5 ms | 5.87 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +7.8 ms | 0.0 ms | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q05 | 1978.0 ms | 1931.0 ms | 0.976x | 47.0 ms | 5.00 ms | yes | 完整 excitation 相同 | -5.9 ms | 0.0 ms | 计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态 |
| q06 | 2460.5 ms | 2434.5 ms | 0.989x | 26.0 ms | 2.85 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -4.8 ms | 0.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q07 | 1740.5 ms | 1670.5 ms | 0.960x | 70.0 ms | 7.26 ms | yes | 边与顺序相同，filter range/type 变化 | -22.9 ms | 0.0 ms | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；主要证据是采样对 DuckDB buffer/decode 路径的预热 |

## Cold wins

### JOB (50 queries)

| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | I/O ratio | Evidence-based explanation |
|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---:|---|
| 02a | 149.0 ms | 138.0 ms | 0.926x | 11.0 ms | 37.05 ms | no | 完整 excitation 相同 | -10.8 ms | +26.8 ms | 1.005x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 02b | 146.0 ms | 138.0 ms | 0.945x | 8.0 ms | 32.20 ms | yes | 完整 excitation 相同 | -9.2 ms | +23.5 ms | 0.989x | 读取量近似相同；并行预取/更早建立 buffer residency 降低等待；计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用 |
| 02d | 164.0 ms | 154.0 ms | 0.939x | 10.0 ms | 33.75 ms | no | 完整 excitation 相同 | -3.0 ms | +30.6 ms | 1.001x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 03a | 714.0 ms | 709.0 ms | 0.993x | 5.0 ms | 36.87 ms | no | 最终计划相同，excitation 轮次/时序变化 | -28.3 ms | +8.6 ms | 1.001x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 04a | 137.0 ms | 124.0 ms | 0.905x | 13.0 ms | 34.20 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -11.2 ms | +23.3 ms | 1.000x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| 04b | 115.0 ms | 109.0 ms | 0.948x | 6.0 ms | 34.24 ms | yes | 完整 excitation 相同 | -3.9 ms | +30.7 ms | 1.033x | 虽多读数据仍更快，收益来自计划时序或并行预取而非读取量；计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用 |
| 04c | 148.0 ms | 143.0 ms | 0.966x | 5.0 ms | 31.64 ms | no | 边/次数不变，最终顺序变化 | -7.3 ms | +24.8 ms | 1.009x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 05c | 657.0 ms | 654.0 ms | 0.995x | 3.0 ms | 44.97 ms | no | 边/次数不变，最终顺序变化 | +132.8 ms | +177.0 ms | 1.018x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 06f | 742.0 ms | 718.0 ms | 0.968x | 24.0 ms | 43.48 ms | no | 边不变，执行次数变化 | -25.5 ms | +16.5 ms | 0.991x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 07c | 1286.0 ms | 1275.0 ms | 0.991x | 11.0 ms | 73.21 ms | no | 最终计划相同，excitation 轮次/时序变化 | +414.0 ms | +488.0 ms | 1.023x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 09b | 494.0 ms | 484.0 ms | 0.980x | 10.0 ms | 82.02 ms | no | 完整 excitation 相同 | -42.4 ms | +39.9 ms | 1.132x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 09c | 635.0 ms | 617.0 ms | 0.972x | 18.0 ms | 72.43 ms | no | 边不变，执行次数变化 | -38.4 ms | +34.3 ms | 1.095x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 09d | 847.0 ms | 845.0 ms | 0.998x | 2.0 ms | 69.21 ms | no | 最终计划相同，excitation 轮次/时序变化 | -45.2 ms | +25.9 ms | 1.097x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 10c | 1357.0 ms | 1341.0 ms | 0.988x | 16.0 ms | 51.60 ms | no | 最终计划相同，excitation 轮次/时序变化 | -47.9 ms | +4.6 ms | 1.292x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 14c | 186.0 ms | 184.0 ms | 0.989x | 2.0 ms | 55.66 ms | no | 最终计划相同，excitation 轮次/时序变化 | -31.8 ms | +24.5 ms | 1.167x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 15a | 440.0 ms | 395.0 ms | 0.898x | 45.0 ms | 66.82 ms | no | 边/次数不变，最终顺序变化 | -181.1 ms | -113.9 ms | 0.692x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；采样页与后续扫描复用或新计划少读数据，物理读取下降；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 15c | 374.0 ms | 361.0 ms | 0.965x | 13.0 ms | 54.29 ms | no | 边不变，执行次数变化 | +42.7 ms | +96.4 ms | 1.121x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 15d | 276.0 ms | 269.0 ms | 0.975x | 7.0 ms | 49.55 ms | no | 最终计划相同，excitation 轮次/时序变化 | -7.4 ms | +42.3 ms | 1.091x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 16a | 558.0 ms | 545.0 ms | 0.977x | 13.0 ms | 54.94 ms | no | 最终计划相同，excitation 轮次/时序变化 | -42.6 ms | +14.1 ms | 1.030x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 16b | 1369.0 ms | 1330.0 ms | 0.972x | 39.0 ms | 52.97 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -30.5 ms | +23.7 ms | 0.990x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| 16c | 667.0 ms | 651.0 ms | 0.976x | 16.0 ms | 55.06 ms | no | 边/次数不变，最终顺序变化 | -34.4 ms | +21.8 ms | 1.021x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 16d | 653.0 ms | 632.0 ms | 0.968x | 21.0 ms | 58.36 ms | no | 边不变，执行次数变化 | -23.3 ms | +36.2 ms | 1.021x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 17a | 378.0 ms | 355.0 ms | 0.939x | 23.0 ms | 48.53 ms | no | 完整 excitation 相同 | -21.2 ms | +29.1 ms | 1.050x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 17c | 337.0 ms | 331.0 ms | 0.982x | 6.0 ms | 50.51 ms | no | 完整 excitation 相同 | -18.1 ms | +33.5 ms | 1.056x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 17d | 680.0 ms | 653.0 ms | 0.960x | 27.0 ms | 45.64 ms | yes | 边不变，执行次数变化 | -22.1 ms | +25.6 ms | 0.985x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| 17e | 864.0 ms | 853.0 ms | 0.987x | 11.0 ms | 44.67 ms | yes | 完整 excitation 相同 | -18.9 ms | +27.5 ms | 0.981x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 17f | 927.0 ms | 893.0 ms | 0.963x | 34.0 ms | 46.55 ms | yes | 边不变，执行次数变化 | +1.6 ms | +48.6 ms | 0.979x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；采样页与后续扫描复用或新计划少读数据，物理读取下降 |
| 18a | 801.0 ms | 776.0 ms | 0.969x | 25.0 ms | 56.98 ms | yes | 边/次数不变，最终顺序变化 | -13.7 ms | +45.5 ms | 1.115x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；虽多读数据仍更快，收益来自计划时序或并行预取而非读取量 |
| 18b | 877.0 ms | 853.0 ms | 0.973x | 24.0 ms | 59.65 ms | yes | 完整 excitation 相同 | -47.3 ms | +13.2 ms | 1.053x | 虽多读数据仍更快，收益来自计划时序或并行预取而非读取量；计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用 |
| 18c | 1171.0 ms | 1146.0 ms | 0.979x | 25.0 ms | 50.40 ms | no | 边/次数不变，最终顺序变化 | -73.6 ms | -24.4 ms | 1.029x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 19a | 488.0 ms | 473.0 ms | 0.969x | 15.0 ms | 82.26 ms | no | 完整 excitation 相同 | -69.4 ms | +12.9 ms | 1.160x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 19c | 660.0 ms | 641.0 ms | 0.971x | 19.0 ms | 81.27 ms | no | 边不变，执行次数变化 | -147.9 ms | -66.8 ms | 1.162x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 20a | 977.0 ms | 969.0 ms | 0.992x | 8.0 ms | 55.81 ms | no | 边/次数不变，最终顺序变化 | -66.0 ms | -9.9 ms | 1.017x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 20c | 1054.0 ms | 1048.0 ms | 0.994x | 6.0 ms | 67.93 ms | no | 边不变，执行次数变化 | -72.6 ms | -4.3 ms | 1.013x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 22a | 277.0 ms | 272.0 ms | 0.982x | 5.0 ms | 69.51 ms | no | 边不变，执行次数变化 | -15.1 ms | +55.7 ms | 1.140x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 22b | 273.0 ms | 259.0 ms | 0.949x | 14.0 ms | 66.44 ms | no | 最终计划相同，excitation 轮次/时序变化 | -30.4 ms | +37.0 ms | 1.156x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 22c | 293.0 ms | 286.0 ms | 0.976x | 7.0 ms | 65.09 ms | no | 边不变，执行次数变化 | -19.7 ms | +46.4 ms | 1.142x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 22d | 249.0 ms | 243.0 ms | 0.976x | 6.0 ms | 64.14 ms | no | 边不变，执行次数变化 | -12.7 ms | +52.7 ms | 1.138x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 23a | 381.0 ms | 316.0 ms | 0.829x | 65.0 ms | 55.76 ms | no | 完整 excitation 相同 | +25.0 ms | +81.5 ms | 0.874x | 采样页与后续扫描复用或新计划少读数据，物理读取下降；计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 23b | 296.0 ms | 210.0 ms | 0.709x | 86.0 ms | 69.65 ms | yes | 边/次数不变，最终顺序变化 | +146.1 ms | +215.9 ms | 1.491x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；虽多读数据仍更快，收益来自计划时序或并行预取而非读取量 |
| 26a | 1053.0 ms | 1034.0 ms | 0.982x | 19.0 ms | 69.76 ms | yes | 边不变，执行次数变化 | -81.0 ms | -10.1 ms | 1.010x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 26b | 914.0 ms | 902.0 ms | 0.987x | 12.0 ms | 66.92 ms | yes | 完整 excitation 相同 | -59.5 ms | +8.9 ms | 1.024x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 26c | 1007.0 ms | 1001.0 ms | 0.994x | 6.0 ms | 63.75 ms | no | 边/次数不变，最终顺序变化 | -66.4 ms | -1.5 ms | 1.021x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 28a | 246.0 ms | 237.0 ms | 0.963x | 9.0 ms | 63.24 ms | no | 边/次数不变，最终顺序变化 | -18.3 ms | +45.6 ms | 1.135x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 28b | 213.0 ms | 183.0 ms | 0.859x | 30.0 ms | 92.53 ms | no | 边不变，执行次数变化 | -16.2 ms | +77.7 ms | 0.965x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；采样页与后续扫描复用或新计划少读数据，物理读取下降；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 28c | 262.0 ms | 256.0 ms | 0.977x | 6.0 ms | 85.27 ms | no | 边/次数不变，最终顺序变化 | -66.5 ms | +19.7 ms | 1.145x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 30a | 979.0 ms | 949.0 ms | 0.969x | 30.0 ms | 73.75 ms | yes | 边不变，执行次数变化 | -26.9 ms | +48.5 ms | 1.001x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| 30c | 1144.0 ms | 1108.0 ms | 0.969x | 36.0 ms | 70.06 ms | yes | 边不变，执行次数变化 | -40.5 ms | +31.2 ms | 1.018x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| 31a | 972.0 ms | 966.0 ms | 0.994x | 6.0 ms | 81.41 ms | no | 边/次数不变，最终顺序变化 | -45.4 ms | +36.5 ms | 1.050x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| 31c | 1011.0 ms | 997.0 ms | 0.986x | 14.0 ms | 79.60 ms | yes | 边/次数不变，最终顺序变化 | -66.9 ms | +13.5 ms | 1.042x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |

### TPC-H SF10 (18 queries)

| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | I/O ratio | Evidence-based explanation |
|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---:|---|
| q01 | 2301.0 ms | 2292.0 ms | 0.996x | 9.0 ms | 0.00 ms | no | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 1.000x | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q02 | 259.0 ms | 255.0 ms | 0.985x | 4.0 ms | 11.15 ms | yes | 边/次数不变，最终顺序变化 | +2.8 ms | +3.4 ms | 1.000x | 采样估计改变了计划，但阶段数据不足以把收益完全归因于计划；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q03 | 1801.0 ms | 1767.0 ms | 0.981x | 34.0 ms | 30.66 ms | no | 完整 excitation 相同 | +21.8 ms | +50.7 ms | 0.986x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q04 | 1583.0 ms | 1575.0 ms | 0.995x | 8.0 ms | 0.00 ms | no | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 1.000x | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q05 | 1909.0 ms | 1857.0 ms | 0.973x | 52.0 ms | 34.18 ms | no | 完整 excitation 相同 | -6.0 ms | +26.9 ms | 0.992x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q07 | 1998.0 ms | 1994.0 ms | 0.998x | 4.0 ms | 31.37 ms | no | 完整 excitation 相同 | +11.8 ms | +46.2 ms | 0.990x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q08 | 2224.0 ms | 2167.0 ms | 0.974x | 57.0 ms | 46.45 ms | yes | 最终计划相同，excitation 轮次/时序变化 | -9.3 ms | +39.6 ms | 1.000x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| q09 | 3392.0 ms | 3321.0 ms | 0.979x | 71.0 ms | 53.39 ms | yes | 边不变，执行次数变化 | +51.1 ms | +75.5 ms | 0.984x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |
| q10 | 2653.0 ms | 2608.0 ms | 0.983x | 45.0 ms | 49.41 ms | yes | 边不变，执行次数变化 | +89.3 ms | +144.7 ms | 0.986x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q11 | 236.0 ms | 231.0 ms | 0.979x | 5.0 ms | 19.77 ms | yes | 完整 excitation 相同 | +11.9 ms | +32.2 ms | 0.936x | 采样页与后续扫描复用或新计划少读数据，物理读取下降；计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用 |
| q12 | 2002.0 ms | 1955.0 ms | 0.977x | 47.0 ms | 24.36 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +7.8 ms | +34.2 ms | 0.977x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；采样页与后续扫描复用或新计划少读数据，物理读取下降 |
| q14 | 1576.0 ms | 1555.0 ms | 0.987x | 21.0 ms | 21.79 ms | no | 完整 excitation 相同 | 0.0 ms | +24.9 ms | 0.991x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q15 | 1479.0 ms | 1478.0 ms | 0.999x | 1.0 ms | 0.00 ms | no | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 1.000x | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |
| q16 | 462.0 ms | 445.0 ms | 0.963x | 17.0 ms | 24.96 ms | yes | 最终计划相同，excitation 轮次/时序变化 | +0.4 ms | +25.6 ms | 0.978x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；采样页与后续扫描复用或新计划少读数据，物理读取下降 |
| q17 | 1694.0 ms | 1681.0 ms | 0.992x | 13.0 ms | 0.89 ms | no | 完整 excitation 相同 | -2.4 ms | -1.3 ms | 1.000x | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q18 | 3378.0 ms | 3343.0 ms | 0.990x | 35.0 ms | 27.82 ms | no | 完整 excitation 相同 | -29.8 ms | 0.0 ms | 0.974x | 采样页与后续扫描复用或新计划少读数据，物理读取下降；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q21 | 2685.0 ms | 2672.0 ms | 0.995x | 13.0 ms | 26.06 ms | no | 完整 excitation 相同 | +38.4 ms | +65.3 ms | 0.987x | 计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q22 | 526.0 ms | 524.0 ms | 0.996x | 2.0 ms | 0.00 ms | no | 完整 excitation 相同 | 0.0 ms | 0.0 ms | 1.000x | 即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动 |

### Appian (6 queries)

| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | I/O ratio | Evidence-based explanation |
|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---:|---|
| q01 | 1484.0 ms | 1471.0 ms | 0.991x | 13.0 ms | 25.60 ms | yes | 完整 excitation 相同 | -26.6 ms | 0.0 ms | 1.002x | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q02 | 3309.0 ms | 3288.0 ms | 0.994x | 21.0 ms | 81.13 ms | no | 边不变，执行次数变化 | -9.4 ms | +70.6 ms | 0.974x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；采样页与后续扫描复用或新计划少读数据，物理读取下降；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q03 | 2871.0 ms | 2836.0 ms | 0.988x | 35.0 ms | 60.12 ms | yes | 边不变，执行次数变化 | +41.4 ms | +101.0 ms | 0.984x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q04 | 11598.0 ms | 11162.0 ms | 0.962x | 436.0 ms | 68.27 ms | no | 完整 excitation 相同 | -41.7 ms | +26.8 ms | 0.979x | 采样页与后续扫描复用或新计划少读数据，物理读取下降；计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用；差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q05 | 2276.0 ms | 2252.0 ms | 0.989x | 24.0 ms | 49.23 ms | yes | 完整 excitation 相同 | -50.9 ms | 0.0 ms | 0.987x | 差值较小或重复区间重叠，运行波动也能解释部分/全部差异 |
| q07 | 2160.0 ms | 2110.0 ms | 0.977x | 50.0 ms | 66.54 ms | yes | 边不变，执行次数变化 | +48.4 ms | +113.2 ms | 0.993x | 采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低；读取量近似相同；并行预取/更早建立 buffer residency 降低等待 |

## Important limitation

The report can establish when a plan changed and when physical input changed. For an identical plan, the current counters cannot split the residual gain exactly among buffer pinning, compression metadata/decode locality, CPU cache state, asynchronous prefetch, and ordinary scheduling noise. Those rows are therefore described as buffer/prefetch evidence or noise-compatible, not as a sampling-accuracy win.
