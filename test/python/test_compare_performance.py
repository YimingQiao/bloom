import unittest
from collections import defaultdict
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from scripts.compare_performance import (
    compare_workload,
    parse_timings,
    report_duckdb_summary,
    report_summary,
    run_once,
    summarize_timings,
)
from scripts import run_benchmark


class ParseTimingsTest(unittest.TestCase):
    def test_parses_complete_runner_output(self):
        output = "\n".join(
            [
                "benchmark/imdb/01a.benchmark\t1\t0.10",
                "benchmark/imdb/01a.benchmark\t2\t0.12",
                "unrelated output",
            ]
        )
        self.assertEqual(
            parse_timings(output, 2),
            {"benchmark/imdb/01a.benchmark": [0.10, 0.12]},
        )

    def test_rejects_incomplete_runner_output(self):
        with self.assertRaisesRegex(RuntimeError, "incomplete benchmark timings"):
            parse_timings("benchmark/imdb/01a.benchmark\t1\t0.10", 2)


class SummarizeTimingsTest(unittest.TestCase):
    def make_timings(self, workload, value):
        count = 113 if workload == "imdb" else 22
        return defaultdict(
            list,
            {f"benchmark/{workload}/q{index:03}.benchmark": [value] * 10 for index in range(count)},
        )

    def test_reports_query_regressions(self):
        base = self.make_timings("tpch_sf1", 1.0)
        candidate = self.make_timings("tpch_sf1", 1.0)
        candidate["benchmark/tpch_sf1/q000.benchmark"] = [1.2] * 10

        summary = summarize_timings("tpch_sf1", base, candidate, 10, 1.10)

        self.assertEqual(len(summary["query_regressions"]), 1)
        self.assertGreater(summary["geomean_ratio"], 1.0)

    def test_rejects_missing_queries(self):
        base = self.make_timings("imdb", 1.0)
        candidate = self.make_timings("imdb", 1.0)
        candidate.pop("benchmark/imdb/q000.benchmark")

        with self.assertRaisesRegex(RuntimeError, "query-set mismatch"):
            summarize_timings("imdb", base, candidate, 10, 1.10)

    def test_geomean_gate_uses_ratio_or_absolute_delta(self):
        cases = [
            (1.0, 1.11, True),
            (1.0, 1.06, True),
            (1.0, 1.04, False),
        ]
        for base, candidate, expected in cases:
            with self.subTest(candidate=candidate), redirect_stdout(StringIO()):
                summary = {
                    "workload": "tpch_sf1",
                    "base_geomean": base,
                    "candidate_geomean": candidate,
                    "geomean_ratio": candidate / base,
                    "query_regressions": [],
                }
                self.assertEqual(report_summary(summary, 1.10, 0.050, False), expected)


class DuckDBBaselineTest(unittest.TestCase):
    def test_disables_rpt_on_candidate_runner(self):
        with TemporaryDirectory() as directory:
            args = SimpleNamespace(out_dir=Path(directory), threads=1, timed_runs=1)
            completed = SimpleNamespace(
                returncode=0,
                stdout="",
                stderr="benchmark/tpch_sf1/q01.benchmark\t1\t0.10\n",
            )
            with (
                patch("scripts.compare_performance.subprocess.run", return_value=completed) as execute,
                redirect_stdout(StringIO()),
            ):
                run_once(args, "tpch_sf1", "duckdb", Path("/candidate/runner"), 0)

            command = execute.call_args.args[0]
            self.assertIn("--baseline", command)
            self.assertIn("/candidate/runner", command)

    def test_alternates_both_comparisons(self):
        timings = defaultdict(
            list,
            {f"benchmark/tpch_sf1/q{index:03}.benchmark": [1.0] for index in range(22)},
        )
        args = SimpleNamespace(
            base_runner=Path("/base/runner"),
            candidate_runner=Path("/candidate/runner"),
            rounds=2,
            timed_runs=1,
            regression_ratio=1.10,
        )
        with patch("scripts.compare_performance.run_once", return_value=timings) as execute:
            compare_workload(args, "tpch_sf1")

        self.assertEqual(
            [call.args[2] for call in execute.call_args_list],
            ["base", "candidate", "duckdb", "duckdb", "candidate", "base"],
        )
        duckdb_calls = [call for call in execute.call_args_list if call.args[2] == "duckdb"]
        self.assertTrue(all(call.args[3] == args.candidate_runner for call in duckdb_calls))

    def test_reports_total_and_geomean_speedup(self):
        summary = {
            "workload": "tpch_sf1",
            "rows": [
                {"base": 2.0, "candidate": 1.0, "ratio": 0.5},
                {"base": 4.0, "candidate": 2.0, "ratio": 0.5},
            ],
            "base_geomean": 2.0,
            "candidate_geomean": 1.0,
            "geomean_ratio": 0.5,
        }
        with redirect_stdout(StringIO()) as output:
            report_duckdb_summary(summary)

        self.assertIn("Bloom vs DuckDB", output.getvalue())
        self.assertEqual(output.getvalue().count("2.000x"), 2)
        self.assertIn("faster on 2/2 queries", output.getvalue())


class BenchmarkGenerationTest(unittest.TestCase):
    def test_cold_job_database_requires_remote_readers(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(run_benchmark, "DATA_DIR", root / "data"):
                run_benchmark.write_benchmark_root(root, "imdb", None, "bloom")

            benchmark = (root / "benchmark" / "imdb" / "01a.benchmark").read_text(encoding="ascii")
            self.assertIn("require bloom", benchmark)
            self.assertIn("require httpfs", benchmark)
            self.assertIn("require parquet", benchmark)
            self.assertIn("load ", benchmark)


if __name__ == "__main__":
    unittest.main()
