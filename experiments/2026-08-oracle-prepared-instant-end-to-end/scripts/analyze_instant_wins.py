#!/usr/bin/env python3
"""Explain every per-query instant-sampling win in the final matrix.

This is an evidence merger, not another benchmark runner.  It combines:

* the final prepared/instant end-to-end repetitions;
* warm prepared/direct-storage transfer signatures and RPT phase timings;
* cold 16-window direct-storage signatures and sampling timings; and
* cold physical input measured by ``ru_inblock``.

The report deliberately separates an observed speedup from a causal claim.  A
different execution signature is plan evidence.  An identical signature with
less physical input is page-reuse evidence.  An identical signature with
overlapping repetitions is reported as noise-compatible rather than credited
to a better estimate.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import statistics
from collections import Counter
from pathlib import Path


SCRIPT = Path(__file__).resolve()
EXPERIMENT = SCRIPT.parents[1]
ROOT = SCRIPT.parents[3]
CROSS = ROOT / "experiments/2026-08-cross-workload-direct-sampling"
FINAL = EXPERIMENT / "results/final"

EXECUTION_SIGNATURE = re.compile(
    r"RPTExecutionSignature hash=([0-9a-f]+) rounds=(\d+) actions=(\d+) canonical=(.*)$",
    re.MULTILINE,
)
PLAN_SIGNATURE = re.compile(
    r"RPTPlanSignature hash=([0-9a-f]+) actions=(\d+) canonical=(.*)$",
    re.MULTILINE,
)
KEY_VALUE = re.compile(r"([A-Za-z_]+)=([^\s]+)")

WORKLOADS = ("job", "tpch_sf10", "appian")
DISPLAY = {"job": "JOB", "tpch_sf10": "TPC-H SF10", "appian": "Appian"}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results",
        type=Path,
        default=FINAL,
        help="Root containing warm/cold workload result directories",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=FINAL / "INSTANT_WINS.md",
        help="Markdown report path",
    )
    return parser.parse_args()


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def by_query(records):
    return {record["query"]: record for record in records}


def edge_only(canonical):
    return re.sub(r"#[^;]*", "", canonical or "")


def action_multiset(canonical):
    return ";".join(sorted(token for token in edge_only(canonical).split(";") if token))


def unique_edges(canonical):
    return ";".join(sorted(set(token for token in edge_only(canonical).split(";") if token)))


def number(value):
    try:
        return float(value.removesuffix("ms"))
    except (AttributeError, ValueError):
        return 0.0


def plan_from_record(record):
    execution_hash = record.get("execution_hash", record.get("execution_signature"))
    if execution_hash in (None, "", "no_execution"):
        execution_hash = None
    plan_hash = record.get("plan_hash", record.get("plan_signature"))
    if plan_hash in (None, "", "no_plan"):
        plan_hash = None
    canonical = record.get("canonical", "")
    edge_plan = record.get("edge_plan", edge_only(canonical))
    return {
        "execution_hash": execution_hash,
        "plan_hash": plan_hash,
        "execution_rounds": int(record.get("execution_rounds", 0)),
        "execution_actions": int(
            record.get("execution_actions", record.get("plan_actions", 0))
        ),
        "edge_plan": edge_plan,
        "action_multiset": record.get("action_multiset", action_multiset(canonical)),
        "unique_edges": record.get("unique_edge_set", unique_edges(canonical)),
        "rpt_total_ms": float(record.get("rpt_total_ms", 0.0)),
        "rpt_materialize_ms": float(record.get("rpt_materialize_ms", 0.0)),
        "rpt_build_bf_ms": float(record.get("rpt_build_bf_ms", 0.0)),
    }


def plan_from_raw(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        text = handle.read()
    executions = EXECUTION_SIGNATURE.findall(text)
    plans = PLAN_SIGNATURE.findall(text)
    execution_hash = None
    execution_rounds = 0
    execution_actions = 0
    if executions:
        execution_hash, rounds, actions, _ = executions[-1]
        execution_rounds = int(rounds)
        execution_actions = int(actions)
    plan_hash = None
    canonical = ""
    if plans:
        plan_hash, _, canonical = plans[-1]
    timing = {}
    for line in text.splitlines():
        if "[RPT-Timing]" in line:
            timing = dict(KEY_VALUE.findall(line))
    return {
        "execution_hash": execution_hash,
        "plan_hash": plan_hash,
        "execution_rounds": execution_rounds,
        "execution_actions": execution_actions,
        "edge_plan": edge_only(canonical),
        "action_multiset": action_multiset(canonical),
        "unique_edges": unique_edges(canonical),
        "rpt_total_ms": number(timing.get("total", "0")),
        "rpt_materialize_ms": number(timing.get("materialize", "0")),
        "rpt_build_bf_ms": number(timing.get("build_bf", "0")),
    }


def load_reference_plans():
    result = {}
    result["job"] = {
        query: plan_from_record(record)
        for query, record in by_query(
            read_jsonl(CROSS / "quality/job_fixed/parsed/reservoir10k.jsonl")
        ).items()
    }
    result["tpch_sf10"] = {
        query: plan_from_record(record)
        for query, record in by_query(
            read_jsonl(CROSS / "quality/tpch_sf10_fixed/parsed/reservoir10k.jsonl")
        ).items()
    }
    result["appian"] = {
        query: plan_from_record(record)
        for query, record in by_query(
            read_jsonl(
                CROSS
                / "end_to_end/sampling_latency/appian/prepared-memory/sampling.jsonl"
            )
        ).items()
    }
    return result


def load_warm_instant_plans():
    result = {}
    result["job"] = {
        query: plan_from_record(record)
        for query, record in by_query(
            read_jsonl(CROSS / "quality/job_fixed/parsed/storage_c256_s2.jsonl")
        ).items()
    }
    result["tpch_sf10"] = {
        query: plan_from_record(record)
        for query, record in by_query(
            read_jsonl(CROSS / "quality/tpch_sf10_fixed/parsed/storage_c256_s2.jsonl")
        ).items()
    }
    result["appian"] = {
        query: plan_from_record(record)
        for query, record in by_query(
            read_jsonl(
                CROSS
                / "end_to_end/sampling_latency/appian/immediate-warm/sampling.jsonl"
            )
        ).items()
    }
    return result


def load_cold_instant_plans():
    result = {}
    for workload in WORKLOADS:
        raw = (
            CROSS
            / f"end_to_end/sampling_latency/{workload}/immediate-cold/raw/prepare/cold"
        )
        result[workload] = {
            path.name.removesuffix("-r1.log.gz"): plan_from_raw(path)
            for path in sorted(raw.glob("*-r1.log.gz"))
        }
    return result


def load_cold_prepared_phases():
    result = {}
    base = EXPERIMENT / "results/diagnostics/cold-prepared-phases"
    for workload in WORKLOADS:
        path = base / workload / "prepared-cold-phases.jsonl"
        result[workload] = {
            query: plan_from_record(record)
            for query, record in by_query(read_jsonl(path)).items()
        }
    return result


def load_sampling_latency(workload, variant):
    base = CROSS / f"end_to_end/sampling_latency/{workload}"
    if variant == "prepared":
        path = base / "prepared-memory/sampling.jsonl"
        field = "sample_effective_ms"
    elif variant == "warm":
        path = base / "immediate-warm/sampling.jsonl"
        field = "sample_effective_ms"
    else:
        path = base / "immediate-cold/cold.jsonl"
        field = "sample_physical_ms"
    grouped = {}
    for record in read_jsonl(path):
        grouped.setdefault(record["query"], []).append(float(record.get(field, 0.0)))
    return {query: statistics.median(values) for query, values in grouped.items()}


def load_final(results_root, state, workload):
    grouped = {}
    for method in ("prepared", "instant"):
        path = results_root / state / workload / method / "results.jsonl"
        for record in read_jsonl(path):
            query = record["query"]
            entry = grouped.setdefault(query, {})
            entry.setdefault(f"{method}_times", []).append(float(record["query_ms"]))
            entry.setdefault(f"{method}_input", []).append(float(record["input_bytes"]))
    for entry in grouped.values():
        for method in ("prepared", "instant"):
            entry[f"{method}_ms"] = statistics.median(entry[f"{method}_times"])
            entry[f"{method}_input_bytes"] = statistics.median(entry[f"{method}_input"])
    return grouped


def relation(reference, instant):
    if reference["execution_hash"] == instant["execution_hash"] and (
        reference["execution_hash"] is not None
        or reference["plan_hash"] == instant["plan_hash"]
    ):
        return "exact", "完整 excitation 相同"
    if not reference["execution_hash"] and not instant["execution_hash"]:
        if reference["plan_hash"] == instant["plan_hash"]:
            return "exact", "两边均无可执行传递计划"
        return "unknown", "无完整 execution signature"
    if reference["unique_edges"] != instant["unique_edges"]:
        return "edges", "传递边集合变化"
    if reference["action_multiset"] != instant["action_multiset"]:
        return "multiplicity", "边不变，执行次数变化"
    if reference["edge_plan"] != instant["edge_plan"]:
        return "order", "边/次数不变，最终顺序变化"
    if reference["plan_hash"] != instant["plan_hash"]:
        return "filters", "边与顺序相同，filter range/type 变化"
    return "rounds", "最终计划相同，excitation 轮次/时序变化"


def explain(
    state,
    ratio,
    gain,
    separated,
    plan_kind,
    input_ratio,
    rpt_gain,
    materialize_gain,
    sample_ms,
):
    meaningful = ratio <= 0.98 and gain >= 5.0
    phase_win = rpt_gain >= 5.0 or materialize_gain >= 5.0
    plan_phase_win = plan_kind != "exact" and phase_win
    lower_input = state == "cold" and input_ratio <= 0.98
    same_input = state == "cold" and 0.98 < input_ratio < 1.02

    reasons = []
    codes = []
    if sample_ms <= 0.05 and plan_kind == "exact":
        reasons.append("即时路径没有采样且执行计划相同，差异只能来自运行顺序/系统波动")
        codes.append("noise-only")
        return "+".join(codes), "；".join(reasons)
    if plan_phase_win:
        reasons.append("采样估计改变 excitation 时序，RPT/materialize 诊断确认其更低")
        codes.append("plan")
    elif plan_kind != "exact":
        reasons.append("采样估计改变了计划，但阶段数据不足以把收益完全归因于计划")
        codes.append("plan-possible")

    if lower_input:
        reasons.append("采样页与后续扫描复用或新计划少读数据，物理读取下降")
        codes.append("io")
    elif state == "cold" and separated and meaningful and same_input:
        reasons.append("读取量近似相同；并行预取/更早建立 buffer residency 降低等待")
        codes.append("prefetch")
    elif state == "cold" and separated and meaningful and input_ratio >= 1.02:
        reasons.append("虽多读数据仍更快，收益来自计划时序或并行预取而非读取量")
        codes.append("latency")

    if state == "warm" and plan_kind == "exact" and separated and meaningful:
        if phase_win:
            reasons.append("计划完全相同但 RPT/materialize 更低，直接采样的 block pin/decode 预热得到阶段计时支持")
        else:
            reasons.append("计划完全相同；残余收益来自 base-block/buffer/decode 预热或调度状态")
        codes.append("buffer")
    elif state == "cold" and plan_kind == "exact" and phase_win:
        reasons.append("计划相同但 cold RPT/materialize 更低，支持采样预取页被正式 materialize 复用")
        codes.append("prefetch")
    elif state == "warm" and plan_kind != "exact" and not plan_phase_win and separated and meaningful:
        reasons.append("主要证据是采样对 DuckDB buffer/decode 路径的预热")
        codes.append("buffer")

    if not separated or not meaningful:
        reasons.append("差值较小或重复区间重叠，运行波动也能解释部分/全部差异")
        codes.append("noise")
    if not reasons:
        reasons.append("观察到稳定小幅优势，但现有计数器不能再细分 cache 与调度效应")
        codes.append("cache/schedule")
    return "+".join(codes), "；".join(reasons)


def fmt_delta(value):
    if abs(value) < 0.05:
        return "0.0"
    return f"{value:+.1f}"


def main():
    args = parse_args()
    results_root = args.results.resolve()
    output = args.output.resolve()
    references = load_reference_plans()
    warm_plans = load_warm_instant_plans()
    cold_plans = load_cold_instant_plans()
    cold_prepared_phases = load_cold_prepared_phases()
    rows = []
    cause_counts = Counter()

    for state in ("warm", "cold"):
        for workload in WORKLOADS:
            final = load_final(results_root, state, workload)
            prepared_sampling = load_sampling_latency(workload, "prepared")
            instant_sampling = load_sampling_latency(workload, state)
            for query, timing in sorted(final.items()):
                prepared_ms = timing["prepared_ms"]
                instant_ms = timing["instant_ms"]
                if instant_ms >= prepared_ms:
                    continue
                reference = references[workload][query]
                instant_plan = (warm_plans if state == "warm" else cold_plans)[workload][query]
                phase_reference = (
                    reference if state == "warm" else cold_prepared_phases[workload][query]
                )
                plan_kind, plan_text = relation(reference, instant_plan)
                ratio = instant_ms / prepared_ms
                gain = prepared_ms - instant_ms
                separated = max(timing["instant_times"]) < min(timing["prepared_times"])
                input_ratio = (
                    timing["instant_input_bytes"] / timing["prepared_input_bytes"]
                    if timing["prepared_input_bytes"]
                    else 1.0
                )
                rpt_gain = phase_reference["rpt_total_ms"] - instant_plan["rpt_total_ms"]
                materialize_gain = (
                    phase_reference["rpt_materialize_ms"]
                    - instant_plan["rpt_materialize_ms"]
                )
                cause, reason = explain(
                    state,
                    ratio,
                    gain,
                    separated,
                    plan_kind,
                    input_ratio,
                    rpt_gain,
                    materialize_gain,
                    instant_sampling.get(query, 0.0),
                )
                cause_counts[(state, cause)] += 1
                rows.append(
                    {
                        "state": state,
                        "workload": workload,
                        "query": query,
                        "prepared_ms": prepared_ms,
                        "instant_ms": instant_ms,
                        "ratio": ratio,
                        "gain": gain,
                        "separated": separated,
                        "sample_ms": instant_sampling.get(query, 0.0),
                        "prepared_sample_ms": prepared_sampling.get(query, 0.0),
                        "input_ratio": input_ratio,
                        "plan": plan_text,
                        "rpt_gain": rpt_gain,
                        "materialize_gain": materialize_gain,
                        "cause": cause,
                        "reason": reason,
                    }
                )

    lines = [
        "# Why instant sampling is faster on individual queries",
        "",
        "This report explains every query whose final median is lower with instant sampling. "
        "It does **not** assume that a lower median means a better estimate. End-to-end time, "
        "repeat separation, transfer signatures, RPT phase timing, isolated sampling latency, "
        "and cold physical input are reported separately.",
        "",
        "## Reading the evidence",
        "",
        "- `Repeat-separated=yes` means every measured Instant repetition was faster than every "
        "Prepared repetition (two repetitions warm, three cold). It is stronger than a median "
        "difference, but is not a confidence interval.",
        "- `RPT gain` and `materialize gain` are state-matched diagnostic Prepared-minus-Instant phase "
        "times. Positive is favorable to Instant. These logs are independent of the logging-disabled "
        "final timer, so use them as mechanism evidence rather than add them to the final delta.",
        "- Cold `I/O ratio` includes sampling and final execution. A value below one means Instant "
        "read fewer physical bytes even after paying for sampling.",
        "- An exact execution-signature match rules out a better transfer plan. Any remaining stable "
        "gain comes from the direct sampler changing buffer/decode/prefetch state, or from runtime "
        "variation.",
        "",
        "## Overall classification",
        "",
        f"There are {sum(row['state'] == 'warm' for row in rows)} warm wins and "
        f"{sum(row['state'] == 'cold' for row in rows)} cold wins. "
        f"{sum(row['separated'] for row in rows)} have non-overlapping repetition ranges; "
        f"{sum(not row['separated'] for row in rows)} remain noise-compatible.",
        "",
        "The recurring mechanisms are:",
        "",
        "1. **Different excitation timing.** The unique directed transfer relationships usually "
        "remain the same, but estimates change which source fires first and whether an edge fires "
        "again. Stronger filters can therefore exist before an expensive materialization.",
        "2. **Base-block preconditioning.** Prepared samples are separate in-memory CDCs and do not "
        "touch base-table blocks. Instant sampling reads base storage through DuckDB's buffer manager; "
        "the same blocks, compression metadata, and decoded paths can be reused by materialization "
        "and final scans. This applies even when the OS page cache is warm because every timed query "
        "uses a fresh DuckDB process/buffer manager.",
        "3. **Cold asynchronous prefetch and overlap.** The disk sampler resolves codec-aware block "
        "handles and prefetches them in parallel. Sampling I/O is not necessarily additive when the "
        "formal scan later needs those blocks.",
        "4. **Noise.** Small median wins with overlapping repetitions are observations, not evidence "
        "that Instant is intrinsically faster.",
        "",
    ]

    for state in ("warm", "cold"):
        lines += [f"## {state.capitalize()} wins", ""]
        for workload in WORKLOADS:
            selected = [
                row for row in rows if row["state"] == state and row["workload"] == workload
            ]
            lines += [f"### {DISPLAY[workload]} ({len(selected)} queries)", ""]
            if state == "warm":
                lines += [
                    "| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | Evidence-based explanation |",
                    "|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---|",
                ]
                for row in selected:
                    lines.append(
                        f"| {row['query']} | {row['prepared_ms']:.1f} ms | "
                        f"{row['instant_ms']:.1f} ms | {row['ratio']:.3f}x | "
                        f"{row['gain']:.1f} ms | {row['sample_ms']:.2f} ms | "
                        f"{'yes' if row['separated'] else 'no'} | {row['plan']} | "
                        f"{fmt_delta(row['rpt_gain'])} ms | "
                        f"{fmt_delta(row['materialize_gain'])} ms | {row['reason']} |"
                    )
            else:
                lines += [
                    "| Query | Prepared | Instant | I/P | Gain | Instant sample | Repeat-separated | Plan relation | RPT gain | Materialize gain | I/O ratio | Evidence-based explanation |",
                    "|---|---:|---:|---:|---:|---:|:---:|---|---:|---:|---:|---|",
                ]
                for row in selected:
                    lines.append(
                        f"| {row['query']} | {row['prepared_ms']:.1f} ms | "
                        f"{row['instant_ms']:.1f} ms | {row['ratio']:.3f}x | "
                        f"{row['gain']:.1f} ms | {row['sample_ms']:.2f} ms | "
                        f"{'yes' if row['separated'] else 'no'} | {row['plan']} | "
                        f"{fmt_delta(row['rpt_gain'])} ms | "
                        f"{fmt_delta(row['materialize_gain'])} ms | "
                        f"{row['input_ratio']:.3f}x | {row['reason']} |"
                    )
            lines.append("")

    lines += [
        "## Important limitation",
        "",
        "The report can establish when a plan changed and when physical input changed. For an "
        "identical plan, the current counters cannot split the residual gain exactly among buffer "
        "pinning, compression metadata/decode locality, CPU cache state, asynchronous prefetch, and "
        "ordinary scheduling noise. Those rows are therefore described as buffer/prefetch evidence "
        "or noise-compatible, not as a sampling-accuracy win.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
