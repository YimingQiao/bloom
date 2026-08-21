import unittest
from collections import defaultdict
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.compare_performance import parse_timings, report_summary, summarize_timings
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
