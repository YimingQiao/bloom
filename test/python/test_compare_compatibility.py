import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.compare_compatibility import compare_results, read_result


def suite_log(*failures):
    sections = []
    for name, line, message in failures:
        sections.append(
            f"error: FAIL {name}\nQuery unexpectedly failed ({name}:{line})\n"
            f"Actual result:\n================\n{message}\n"
        )
    failed = f"{len(failures)} failed, " if failures else ""
    return "\n".join(sections) + f"\nran tests: 6200 passed, {failed}173 skipped in 500s\n"


class CompatibilityComparisonTest(unittest.TestCase):
    def read(self, text, status):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "suite.log"
            path.write_text(text, encoding="utf-8")
            return read_result(path, status)

    def test_accepts_completed_passing_suites(self):
        result = self.read(suite_log(), 0)
        self.assertEqual(compare_results(result, result), [])

    def test_accepts_same_native_oom_with_different_allocation_sizes(self):
        baseline = self.read(suite_log(("memory.test", 12, "Out of Memory Error: 32 MB used")), 1)
        enabled = self.read(suite_log(("memory.test", 12, "Out of Memory Error: 31 MB used")), 1)
        self.assertEqual(compare_results(baseline, enabled), [])

    def test_accepts_native_fetch_error_format(self):
        log = suite_log(("fetch.test", 35, "Out of Memory Error: allocation failed"))
        log = log.replace("failed (", "failed! (").replace("Actual result:\n", "")
        result = self.read(log, 1)
        self.assertEqual(compare_results(result, result), [])

    def test_rejects_mixed_failures_within_one_test(self):
        log = suite_log(("mixed.test", 12, "Out of Memory Error: allocation failed"))
        extra = (
            "Query unexpectedly failed (mixed.test:20)\nActual result:\n================\nBinder Error: missing table\n"
        )
        log = log.replace("ran tests:", extra + "ran tests:")
        result = self.read(log, 1)
        self.assertTrue(compare_results(result, result))

    def test_rejects_new_failure_or_different_assertion(self):
        baseline = self.read(suite_log(("memory.test", 12, "Out of Memory Error: allocation failed")), 1)
        for name, line in (("other.test", 12), ("memory.test", 20)):
            with self.subTest(name=name, line=line):
                enabled = self.read(suite_log((name, line, "Out of Memory Error: allocation failed")), 1)
                self.assertTrue(compare_results(baseline, enabled))

    def test_does_not_whitelist_unknown_shared_errors(self):
        for message in (
            "INTERNAL Error: invalid binding",
            "Wrong result in query",
            "unexpected error",
        ):
            with self.subTest(message=message):
                result = self.read(suite_log(("broken.test", 12, message)), 1)
                self.assertTrue(compare_results(result, result))

    def test_rejects_interrupted_or_incomplete_runs(self):
        for text, status in (
            ("error: FAIL memory.test", 1),
            (suite_log(), 143),
            (suite_log(), 1),
            (suite_log().replace("6200", "12"), 0),
            (suite_log().replace("173 skipped", "1 failed, 173 skipped"), 1),
        ):
            with self.subTest(text=text, status=status), self.assertRaises(ValueError):
                self.read(text, status)


if __name__ == "__main__":
    unittest.main()
