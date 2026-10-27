#!/usr/bin/env python3
"""Compare completed upstream suites, separating reproduced native OOM failures."""

import argparse
import os
import re
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
SUMMARY = re.compile(r"ran tests: (\d+) passed, (?:(\d+) failed, )?(\d+) skipped")
FAILURE = re.compile(r"^error: FAIL (.+)$", re.MULTILINE)


def read_result(path, exit_code):
    output = ANSI.sub("", path.read_text(encoding="utf-8"))
    summaries = SUMMARY.findall(output)
    if exit_code not in (0, 1) or len(summaries) != 1:
        raise ValueError(f"{path}: suite did not complete normally (exit {exit_code})")
    passed, failed, skipped = (int(value or 0) for value in summaries[0])
    if passed < 1000 or bool(failed) != bool(exit_code):
        raise ValueError(f"{path}: incomplete coverage or inconsistent exit status")

    failures = {}
    headers = list(FAILURE.finditer(output))
    for index, header in enumerate(headers):
        name = header.group(1)
        end = headers[index + 1].start() if index + 1 < len(headers) else len(output)
        detail = output[header.end() : end]
        assertions = list(re.finditer(r"Query unexpectedly failed!? \((.+:\d+)\)", detail))
        only_oom = bool(assertions)
        for position, assertion in enumerate(assertions):
            stop = assertions[position + 1].start() if position + 1 < len(assertions) else len(detail)
            message = detail[assertion.end() : stop]
            actual_oom = re.search(r"\n=+\nOut of Memory Error:", message)
            other_error = re.search(r"Wrong result in query|INTERNAL Error|Segmentation fault", message)
            only_oom &= bool(actual_oom) and not other_error
        # Compare assertion locations as well as error kind. Unknown failures
        # remain fatal, even if both configurations failed the same test file.
        signature = frozenset(match.group(1) for match in assertions) if only_oom else None
        if name in failures:
            raise ValueError(f"{path}: repeated failure for {name}; disable test retries")
        failures[name] = signature
    if len(failures) != failed:
        raise ValueError(f"{path}: failure details do not match the completed summary")
    return {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "failures": failures,
    }


def compare_results(disabled, enabled):
    problems = []
    for name, signature in disabled["failures"].items():
        if signature is None:
            problems.append(f"Native baseline has an unclassified failure: {name}")
    for name, signature in enabled["failures"].items():
        if signature is None or disabled["failures"].get(name) != signature:
            problems.append(f"Bloom has a new or different failure: {name}")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for mode in ("disabled", "enabled"):
        parser.add_argument(f"--{mode}-log", type=Path, required=True)
        parser.add_argument(f"--{mode}-exit", type=int, required=True)
    args = parser.parse_args()
    try:
        disabled = read_result(args.disabled_log, args.disabled_exit)
        enabled = read_result(args.enabled_log, args.enabled_exit)
    except ValueError as error:
        print(f"::error::{error}")
        return 1
    problems = compare_results(disabled, enabled)
    lines = [
        "## Full DuckDB compatibility comparison",
        "",
        "| Bloom | Passed | Failed | Skipped |",
        "| --- | ---: | ---: | ---: |",
        *[
            f"| {mode} | {result['passed']} | {result['failed']} | {result['skipped']} |"
            for mode, result in (("Disabled", disabled), ("Enabled", enabled))
        ],
        "",
    ]
    if disabled["failures"]:
        lines += [
            "Native baseline failures remain; this is not an entirely passing upstream suite.",
            "",
        ]
        lines += [f"- `{name}`" for name in sorted(disabled["failures"])]
        lines.append("")
    lines += problems or ["No additional Bloom failures detected."]
    report = "\n".join(lines) + "\n"
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as summary:
            summary.write(report)
    for problem in problems:
        print(f"::error::{problem}")
    return int(bool(problems))


if __name__ == "__main__":
    raise SystemExit(main())
