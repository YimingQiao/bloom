# Bloom 完整 join key 的 L/C 血缘实验

结论：这版引擎原型通过了查询结果检查，但没有显示出总体性能收益。L/C 能改善部分查询的过滤和物化；初始全表激活、pending 过滤的实际落实，以及估计误差，也会增加工作。这个结果不能解读为对血缘记录本身的独立消融。

## 分支与实现

- 仓库/worktree：`/home/yiming/projects/bloom-key-lineage`。
- 分支：`experiment/key-lineage-lc-20261021`，基于 `9a88899`；主要功能提交 `a536e07`，实验脚本提交 `7e87b12`。
- 设置：`SET bloom_key_lineage=true`。默认仍使用旧实现。
- 每个完整 key 分开记录结果表达式 L 和已满足约束 C；收到 pending filter 时不提前更新 C。先实际过滤，再提交状态和决定是否广播。
- 同键/包含键可以合并表达式；跨键删行产生不可变的新投影 token。多个 pending filters 合并处理时使用保守更新。
- Bloom 的接受条件与精确源投影分开记录；不会把假阳性误当作已经满足源表的精确约束。
- 新模式每表初始激活一次，之后根据实际删行广播；采样仍用于选处理顺序。因而比较的是完整执行策略，不是只替换一个 metadata 容器。

## 本轮结果

耗时是各查询三次计时中位数的总和，单位秒；每个查询另有一次预热。物化量统计初始物化和 compaction 实际写出的元组，单位百万行。构建数是实际构建的 transfer filter 数量，包括精确 bitmap/prefix-range 和 Bloom。

| Benchmark | 完整配对查询 | 旧模式耗时 | L/C 耗时 | 耗时变化 | 旧物化量 | L/C 物化量 | Filter 构建：旧 / 新 |
|---|---:|---:|---:|---:|---:|---:|---:|
| JOB | 113/113 | 26.686 | 27.447 | +2.9% | 24.498 | 25.810 | 2074 / 2690 |
| STATS-CEB | 142/146 | 48.822 | 49.069 | +0.5% | 25.830 | 28.850 | 751 / 1417 |
| TPC-H-SF1 | 22/22 | 1.862 | 1.966 | +5.6% | 4.147 | 13.398 | 88 / 102 |

STATS-CEB 的 58、120、122、126 在关闭 Bloom 的参考执行中超过 20 秒限制，未计入配对汇总。因此 STATS-CEB 这一行是 142 条查询的结果，不是整个 146-query workload 的总耗时。277 条配对查询中，旧、新模式共 554 份输出，离线再次比较完整结果多重集，全部与关闭 Bloom 的结果逐值一致。

不同 workload 的 runner 曾并行运行；每个查询只使用一个线程，未绑核。因此 0.5% 或几个百分点的时间差不能视为统计上确定的收益/退化。没有观察到总体提速，是本轮可以支持的结论。所有纳入汇总的运行均没有触发 Bloom 内存预算停传。

## 两个解释性案例

**TPC-H Q18：选择顺序的代价。** 旧模式物化 149,999 行；L/C 物化 9,251,837 行。日志显示，实际只有 57 行的子查询被估成约 6,001,215 行，晚于 orders 和 lineitem 处理；于是先物化大表，随后才应用强过滤。三次计时的中位数是 0.217 / 0.320 秒；另做七次计时复查，得到 0.216 / 0.347 秒。退化是稳定的，几乎解释了 TPC-H 总耗时差异。

**STATS-CEB 138：更多 filter 可以减少物化。** 物化行数从 131,011 降至 16,955，减少 87.1%；构建数从 7 增至 9。端到端时间变化较小。这说明局部传递质量改善存在，但不自动等于整个 workload 提速。

噪声检查也很必要：JOB 15a 首轮看似慢约 30%，七次复查后约慢 3%；07c 首轮约慢 19%，复查后约慢 5%。没有把这些单轮大幅波动当作稳定结论。

## 正确性与范围

- 旧、新模式各通过原有 29 项 SQLLogicTests（每种模式 785 个断言），覆盖低内存、并行过滤、NULL 等。
- 新增 `test/sql/key_lineage.test`，25 个断言，覆盖联合键、跨键影响、重复行、NULL、数据更新后重新规划以及空关系。
- C++ 不变量测试覆盖 no-shrink 时 L 不变/C 增长、pending 尚未满足、a→ab 包含、ab→a 非包含、新投影 token、不可变快照以及 alias 独立状态。
- 70 组针对性/随机 SQL，分别运行关闭 Bloom、旧血缘、L/C，结果一致；随机部分包含 3–5 张表、复合键和 NULL。
- 真实查询检查保证的是最终 SQL 结果一致，未验证 Bloom 执行已达到精确 full reduction。保护区域、环、NULL、假阳性和内存停传不在精确模型保证内。
- 物化计数是物理实现中观察到的元组写出量，不是论文抽象 `2O` 最优代价，也不是内存字节流量。

## 实验配置与版本

DuckDB 固定在 `21aca0424f1faf78b593b1e6fbfdd4846624c987`；同一二进制切换模式。使用 instant sampling，seed=2，threads=1，DuckDB 内存上限 16GB，Bloom 预算 4GB。查询 SQL 不改写，数据库只读打开，未迁移大数据或缓存。硬件为 Xeon Platinum 8474C，详细配置见 `results/hardware.txt`。

主性能结果对应 `results/engine-v2` 和三个 `results/bounded-*` 目录中的二进制 SHA 与源代码快照（功能代码对应 `7e87b12`）。主测试结束后，额外加固了一个内存准入边界：有 pending L/C 更新时，不能用旧投影的 filter-cache 命中省掉新 filter 的预算。这一补丁的验证单独保存在 `results/budget-guard-validation`，未混入上表计时。

## 文件与复现

- `code/run.py`：结果校验、诊断日志、native runner 计时、超时记录；分支中也保存了脚本副本。
- `code/analyze.py`：只汇总两种模式都完成的查询。
- `results/paired-summary.json`：配对汇总。
- `results/paired-queries.csv`：每条 query 的时间、物化量和 filter 构建数。
- `results/strict-output-audit.json`：554 份完整结果的严格复核。
- `results/failures.json`：超时名单。
- `results/bounded-*/logs`：压缩诊断日志、计时日志和逐次计时；继承记录的位置见 `inherited-records.json`。
- `results/recheck`：Q18、15a、07c 的七次计时复查。
- `results/engine-v2`：单元测试、SQL 测试、70 组用例和最初执行的部分 benchmark。

```sh
python3 code/run.py --root /home/yiming/projects/bloom-key-lineage \
  --out results/NEW-RUN --fixtures --runs 3 --timeout 20
python3 code/analyze.py
```

下一步最有价值的是保持相同调度做血缘剪枝消融，再分别分析初始激活和 pending-filter 落实的成本；当前结果尚不足以把新模式直接作为性能改进启用。
