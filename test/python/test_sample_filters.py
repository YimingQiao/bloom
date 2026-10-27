"""Check sample survivors as well as SQL results using the built Bloom CLI."""

import csv
import os
import re
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[2]
DUCKDB = Path(os.environ.get("BLOOM_TEST_DUCKDB", ROOT / "build/release/duckdb"))


@unittest.skipUnless(DUCKDB.is_file(), "Build the Bloom DuckDB CLI first")
class SampleFilterTest(unittest.TestCase):
    def check_filter(self, predicate, mode, *, columns=0, skipped=False, reverse=False, generated=False):
        with TemporaryDirectory(prefix="bloom-sample-filter-") as directory:
            # Storage order, scan projection order and filter input order differ.
            # An unused column also makes storage indexes non-contiguous.
            fields = [
                "(i % 9)::INTEGER y",
                "'unused' filler",
                "i::INTEGER id",
                "CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 5)::INTEGER END x",
                "CASE WHEN i % 3 = 0 THEN 'yes' ELSE 'no' END s",
            ]
            if reverse:
                fields.reverse()
            generated_setup = ""
            if generated:
                generated_setup = """
                CREATE TABLE generated_subject (
                    g AS (id * 5), id INTEGER, y INTEGER, h AS (y + 1), x INTEGER, s VARCHAR
                );
                INSERT INTO generated_subject SELECT id, y, x, s FROM subject;
                DROP TABLE subject;
                ALTER TABLE generated_subject RENAME TO subject;
                """
            sample_mode = "prepared" if mode == "prepared" else "instant"
            access = "block" if mode == "block" else "scattered"
            directory = directory.replace("'", "''")
            source = "subject"
            export = ""
            if mode == "parquet":
                source = f"read_parquet('{directory}/subject.parquet')"
                export = f"COPY subject TO '{directory}/subject.parquet';"
            query = f"""
                SELECT 'RESULT', count(*), coalesce(sum(a.id), 0)
                FROM {source} a JOIN probe b USING (id) WHERE {predicate};
            """
            sql = f"""
                SET threads = 1;
                SET enable_bloom = false;
                SET bloom_sample_cache_dir = '{directory}/samples';
                SET bloom_sample_mode = '{sample_mode}';
                SET bloom_sample_size = 10000;
                SET bloom_instant_access = '{access}';
                SET bloom_instant_access_points = 1;
                SET bloom_instant_rows_per_access = 1000;
                CREATE TABLE probe AS SELECT i::INTEGER id FROM range(100) r(i);
                CREATE TABLE subject AS SELECT {', '.join(fields)} FROM range(100) r(i);
                {generated_setup}
                CHECKPOINT;
                {export}
                {query}
                SET enable_bloom = true;
                SET bloom_log_transfer_steps = true;
                {query}
            """
            result = subprocess.run(
                [str(DUCKDB), "-csv", "-noheader", "-bail"],
                input=sql,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [row for row in csv.reader(result.stdout.splitlines()) if row and row[0] == "RESULT"]
        self.assertEqual(len(rows), 2, result.stdout)
        self.assertEqual(rows[0], rows[1], result.stderr)
        # Sampling covers all 100 rows, so its local survivor count must equal
        # native execution, not merely produce an ultimately correct query.
        table = "READ_PARQUET" if mode == "parquet" else "subject"
        estimates = re.findall(
            rf"\[Bloom-SampleEstimate\] table={table} sample_rows=(\d+) "
            r"local_survivors=(\d+).*?filters=0 status=sampled",
            result.stderr,
        )
        expected = 100 if skipped else int(rows[0][1])
        self.assertTrue(estimates, result.stderr)
        self.assertTrue(
            all((int(total), int(survivors)) == (100, expected) for total, survivors in estimates),
            result.stderr,
        )
        if columns:
            self.assertRegex(
                result.stderr,
                rf"\[TableFilter\] columns={columns} .* (APPLIED|REUSED)",
            )
        if skipped:
            self.assertIn("SKIPPED(unbound)", result.stderr)

    def test_multi_column_sample_survivors(self):
        cases = [
            ("a.x < a.y", 2),
            ("a.x = 1 OR a.s = 'yes'", 2),
            ("a.x = 1 OR a.y = 2", 2),
            ("(a.x = 1 AND a.s = 'yes') OR a.y = 2", 3),
            ("CAST(a.x AS BIGINT) < a.y", 2),
        ]
        for mode in ("prepared", "scattered", "block"):
            for reverse in (False, True):
                for predicate, columns in cases:
                    with self.subTest(mode=mode, reverse=reverse, predicate=predicate):
                        self.check_filter(predicate, mode, columns=columns, reverse=reverse)

    def test_missing_column_skips_whole_or(self):
        # rowid is not sampled. Applying just x=1 would drop rows accepted by
        # the other OR arm, so the sample must retain every row instead.
        for mode in ("prepared", "scattered", "block"):
            with self.subTest(mode=mode):
                self.check_filter("a.x = 1 OR a.rowid = 0", mode, skipped=True)

    def test_parquet_sample_layout(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                self.check_filter("a.x = 1 OR a.s = 'yes'", "parquet", reverse=reverse)

    def test_generated_columns_preserve_sample_layout(self):
        for mode in ("prepared", "scattered", "block"):
            for predicate in ("a.x < a.y", "a.x = 1 OR a.y = 2"):
                with self.subTest(mode=mode, predicate=predicate):
                    self.check_filter(predicate, mode, columns=2, generated=True)

    def test_single_column_and_null_predicates(self):
        for mode in ("prepared", "scattered", "block"):
            for predicate in ("a.x = 1", "a.x IS NULL", "a.x IS NULL OR a.y = 2"):
                with self.subTest(mode=mode, predicate=predicate):
                    self.check_filter(predicate, mode)


if __name__ == "__main__":
    unittest.main()
