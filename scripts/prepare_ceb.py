#!/usr/bin/env python3
"""Download and prepare the CEB IMDb SQL workloads.

The CEB repository's original Dropbox links are no longer reliable.  The same
plain SQL workloads are mirrored in Ryan Marcus's IMDb/JOB dataset repository.
This script pins that mirror to a commit and extracts only the two CEB IMDb
directories into Bloom's gitignored benchmark cache.
"""

import argparse
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parent.parent
CACHE_ROOT = ROOT / ".bench_cache" / "ceb"
SOURCE_REPOSITORY = "https://github.com/RyanMarcus/imdb_pg_dataset"
SOURCE_COMMIT = "1f39e9aa85ee64249f60bfa59543e8707b228644"
SOURCE_URL = f"https://codeload.github.com/RyanMarcus/imdb_pg_dataset/tar.gz/{SOURCE_COMMIT}"
SOURCE_ARCHIVE_SHA256 = "43f4b5984db5b281968a3f548a93cb00cbd8bad7850ce366641592117958754c"
ARCHIVE_PATH = CACHE_ROOT / "downloads" / f"imdb_pg_dataset-{SOURCE_COMMIT}.tar.gz"
QUERY_ROOT = CACHE_ROOT / "queries" / SOURCE_COMMIT
MANIFEST_PATH = QUERY_ROOT / "manifest.json"

WORKLOADS = {
    "ceb-imdb-3k": 3133,
    "ceb-imdb-13k": 13646,
}


def _query_counts():
    return {
        workload: sum(1 for _ in (QUERY_ROOT / workload).glob("*/*.sql"))
        for workload in WORKLOADS
    }


def is_prepared():
    if not MANIFEST_PATH.is_file():
        return False
    return _query_counts() == WORKLOADS


def _download_archive():
    if ARCHIVE_PATH.is_file():
        if _archive_sha256() != SOURCE_ARCHIVE_SHA256:
            raise RuntimeError(f"cached CEB archive failed checksum validation: {ARCHIVE_PATH}")
        return
    ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = ARCHIVE_PATH.with_suffix(".download")
    request = urllib.request.Request(
        SOURCE_URL,
        headers={"User-Agent": "Bloom-CEB-workload-preparer"},
    )
    print(f"Downloading CEB IMDb SQL workloads from {SOURCE_REPOSITORY}")
    try:
        with urllib.request.urlopen(request) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        digest = _file_sha256(temporary)
        if digest != SOURCE_ARCHIVE_SHA256:
            raise RuntimeError(
                f"downloaded CEB archive has SHA-256 {digest}, expected {SOURCE_ARCHIVE_SHA256}"
            )
        os.replace(temporary, ARCHIVE_PATH)
    finally:
        temporary.unlink(missing_ok=True)


def _file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as archive:
        for block in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _archive_sha256():
    return _file_sha256(ARCHIVE_PATH)


def _extract_queries():
    QUERY_ROOT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ceb-queries-", dir=QUERY_ROOT.parent) as temporary:
        staging = Path(temporary)
        counts = {workload: 0 for workload in WORKLOADS}
        with tarfile.open(ARCHIVE_PATH, "r:gz") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                parts = PurePosixPath(member.name).parts
                if len(parts) < 4:
                    continue
                workload = parts[1]
                if workload not in WORKLOADS or not member.name.endswith(".sql"):
                    continue
                relative = Path(*parts[2:])
                destination = staging / workload / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeError(f"could not extract {member.name}")
                with source, destination.open("wb") as output:
                    shutil.copyfileobj(source, output)
                counts[workload] += 1

        if counts != WORKLOADS:
            raise RuntimeError(f"unexpected CEB query counts: {counts}, expected {WORKLOADS}")

        manifest = {
            "source_repository": SOURCE_REPOSITORY,
            "source_commit": SOURCE_COMMIT,
            "source_archive_sha256": _archive_sha256(),
            "workloads": counts,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if QUERY_ROOT.exists():
            shutil.rmtree(QUERY_ROOT)
        os.replace(staging, QUERY_ROOT)


def prepare():
    if is_prepared():
        counts = _query_counts()
        print(
            "CEB IMDb SQL workloads already prepared: "
            + ", ".join(f"{name}={count}" for name, count in counts.items())
        )
        return QUERY_ROOT
    _download_archive()
    _extract_queries()
    counts = _query_counts()
    print(
        "Prepared CEB IMDb SQL workloads: "
        + ", ".join(f"{name}={count}" for name, count in counts.items())
    )
    return QUERY_ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--print-root",
        action="store_true",
        help="Print the prepared query root after preparation",
    )
    args = parser.parse_args()
    root = prepare()
    if args.print_root:
        print(root)


if __name__ == "__main__":
    main()
