#!/usr/bin/env python3
"""Download and prepare the STATS-CEB workload and DuckDB database."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parent.parent
CACHE_ROOT = ROOT / ".bench_cache" / "stats_ceb"
DATA_DIR = ROOT / ".bench_cache" / "data"
SOURCE_REPOSITORY = "https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark"
SOURCE_COMMIT = "670cb8d4bf4cbfa32f94fdf17f33973d3fd67d1b"
SOURCE_URL = (
    "https://codeload.github.com/Nathaniel-Han/"
    f"End-to-End-CardEst-Benchmark/tar.gz/{SOURCE_COMMIT}"
)
SOURCE_ARCHIVE_SHA256 = "ecdc919ddaeabea8cd2437f0faa90b1ebf973d4398955f877f325e79c2235b24"
ARCHIVE_PATH = CACHE_ROOT / "downloads" / f"stats-ceb-{SOURCE_COMMIT}.tar.gz"
ASSET_ROOT = CACHE_ROOT / "assets" / SOURCE_COMMIT
CSV_ROOT = ASSET_ROOT / "data"
QUERY_ROOT = ASSET_ROOT / "queries"
ANSWER_ROOT = ASSET_ROOT / "answers"
MANIFEST_PATH = ASSET_ROOT / "manifest.json"
DATABASE_PATH = DATA_DIR / "stats_ceb.duckdb"
EXPECTED_QUERIES = 146

TABLES = {
    "users": """
        CREATE TABLE users (
            Id INTEGER PRIMARY KEY, Reputation INTEGER, CreationDate TIMESTAMP,
            Views INTEGER, UpVotes INTEGER, DownVotes INTEGER
        )
    """,
    "posts": """
        CREATE TABLE posts (
            Id INTEGER PRIMARY KEY, PostTypeId SMALLINT, CreationDate TIMESTAMP,
            Score INTEGER, ViewCount INTEGER, OwnerUserId INTEGER,
            AnswerCount INTEGER, CommentCount INTEGER, FavoriteCount INTEGER,
            LastEditorUserId INTEGER
        )
    """,
    "postLinks": """
        CREATE TABLE postLinks (
            Id INTEGER PRIMARY KEY, CreationDate TIMESTAMP, PostId INTEGER,
            RelatedPostId INTEGER, LinkTypeId SMALLINT
        )
    """,
    "postHistory": """
        CREATE TABLE postHistory (
            Id INTEGER PRIMARY KEY, PostHistoryTypeId SMALLINT, PostId INTEGER,
            CreationDate TIMESTAMP, UserId INTEGER
        )
    """,
    "comments": """
        CREATE TABLE comments (
            Id INTEGER PRIMARY KEY, PostId INTEGER, Score SMALLINT,
            CreationDate TIMESTAMP, UserId INTEGER
        )
    """,
    "votes": """
        CREATE TABLE votes (
            Id INTEGER PRIMARY KEY, PostId INTEGER, VoteTypeId SMALLINT,
            CreationDate TIMESTAMP, UserId INTEGER, BountyAmount SMALLINT
        )
    """,
    "badges": """
        CREATE TABLE badges (
            Id INTEGER PRIMARY KEY, UserId INTEGER, Date TIMESTAMP
        )
    """,
    "tags": """
        CREATE TABLE tags (
            Id INTEGER PRIMARY KEY, Count INTEGER, ExcerptPostId INTEGER
        )
    """,
}

CSV_NAMES = {
    "users": "users.csv",
    "posts": "posts.csv",
    "postLinks": "postLinks.csv",
    "postHistory": "postHistory.csv",
    "comments": "comments.csv",
    "votes": "votes.csv",
    "badges": "badges.csv",
    "tags": "tags.csv",
}


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_archive():
    if ARCHIVE_PATH.is_file():
        digest = file_sha256(ARCHIVE_PATH)
        if digest != SOURCE_ARCHIVE_SHA256:
            raise RuntimeError(
                f"cached STATS-CEB archive has SHA-256 {digest}, "
                f"expected {SOURCE_ARCHIVE_SHA256}"
            )
        return

    ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = ARCHIVE_PATH.with_suffix(".download")
    request = urllib.request.Request(
        SOURCE_URL,
        headers={"User-Agent": "Bloom-STATS-CEB-workload-preparer"},
    )
    print(f"Downloading STATS-CEB from {SOURCE_REPOSITORY}")
    try:
        with urllib.request.urlopen(request) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        digest = file_sha256(temporary)
        if digest != SOURCE_ARCHIVE_SHA256:
            raise RuntimeError(
                f"downloaded STATS-CEB archive has SHA-256 {digest}, "
                f"expected {SOURCE_ARCHIVE_SHA256}"
            )
        os.replace(temporary, ARCHIVE_PATH)
    finally:
        temporary.unlink(missing_ok=True)


def prepared_assets():
    if not MANIFEST_PATH.is_file():
        return False
    query_count = sum(1 for _ in QUERY_ROOT.glob("*.sql"))
    answer_count = sum(1 for _ in ANSWER_ROOT.glob("*.csv"))
    return (
        query_count == EXPECTED_QUERIES
        and answer_count == EXPECTED_QUERIES
        and all((CSV_ROOT / name).is_file() for name in CSV_NAMES.values())
    )


def extract_assets():
    ASSET_ROOT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stats-ceb-", dir=ASSET_ROOT.parent) as temporary:
        staging = Path(temporary)
        csv_root = staging / "data"
        query_root = staging / "queries"
        answer_root = staging / "answers"
        csv_root.mkdir()
        query_root.mkdir()
        answer_root.mkdir()

        workload_lines = None
        with tarfile.open(ARCHIVE_PATH, "r:gz") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                parts = PurePosixPath(member.name).parts
                if len(parts) == 4 and parts[1:3] == ("datasets", "stats_simplified"):
                    if parts[3] not in CSV_NAMES.values():
                        continue
                    source = archive.extractfile(member)
                    if source is None:
                        raise RuntimeError(f"could not extract {member.name}")
                    with source, (csv_root / parts[3]).open("wb") as output:
                        shutil.copyfileobj(source, output)
                elif parts[1:] == ("workloads", "stats_CEB", "stats_CEB.sql"):
                    source = archive.extractfile(member)
                    if source is None:
                        raise RuntimeError(f"could not extract {member.name}")
                    with source:
                        workload_lines = source.read().decode("utf-8").splitlines()

        if workload_lines is None:
            raise RuntimeError("STATS-CEB SQL workload not found in source archive")
        if len(workload_lines) != EXPECTED_QUERIES:
            raise RuntimeError(
                f"unexpected STATS-CEB query count: {len(workload_lines)}, "
                f"expected {EXPECTED_QUERIES}"
            )

        for query_number, line in enumerate(workload_lines, 1):
            cardinality, separator, sql = line.partition("||")
            if not separator or not cardinality.isdigit() or not sql.strip():
                raise RuntimeError(f"invalid STATS-CEB workload line {query_number}")
            (query_root / f"{query_number}.sql").write_text(
                sql.strip() + "\n", encoding="utf-8"
            )
            (answer_root / f"{query_number}.csv").write_text(
                f"count_star()\n{cardinality}\n", encoding="utf-8"
            )

        manifest = {
            "source_repository": SOURCE_REPOSITORY,
            "source_commit": SOURCE_COMMIT,
            "source_archive_sha256": file_sha256(ARCHIVE_PATH),
            "queries": EXPECTED_QUERIES,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if ASSET_ROOT.exists():
            shutil.rmtree(ASSET_ROOT)
        os.replace(staging, ASSET_ROOT)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def build_database(duckdb, database=DATABASE_PATH):
    database = Path(database).resolve()
    if database.is_file():
        return
    if not duckdb.is_file():
        raise RuntimeError(f"DuckDB CLI not found: {duckdb}")

    database.parent.mkdir(parents=True, exist_ok=True)
    temporary = database.with_suffix(".building")
    temporary.unlink(missing_ok=True)
    statements = []
    for table, ddl in TABLES.items():
        statements.append(ddl.strip() + ";")
        csv_path = (CSV_ROOT / CSV_NAMES[table]).resolve()
        statements.append(
            f"COPY {table} FROM {sql_string(csv_path)} (FORMAT CSV, HEADER);"
        )
    statements.append("CHECKPOINT;")
    try:
        result = subprocess.run(
            [str(duckdb), str(temporary)],
            input="\n".join(statements) + "\n",
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"STATS-CEB database build failed:\n{result.stderr}")
        os.replace(temporary, database)
    finally:
        temporary.unlink(missing_ok=True)


def prepare(duckdb=None, build_db=True, database=DATABASE_PATH):
    if not prepared_assets():
        download_archive()
        extract_assets()
    if build_db:
        duckdb = Path(duckdb or ROOT / "build" / "release" / "duckdb").resolve()
        build_database(duckdb, database)
    print(
        f"STATS-CEB prepared: queries={EXPECTED_QUERIES}, "
        f"database={Path(database).resolve() if build_db else 'external'}"
    )
    return QUERY_ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb",
        type=Path,
        default=ROOT / "build" / "release" / "duckdb",
    )
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--queries-only", action="store_true")
    parser.add_argument("--print-root", action="store_true")
    args = parser.parse_args()
    query_root = prepare(
        args.duckdb,
        build_db=not args.queries_only,
        database=args.database,
    )
    if args.print_root:
        print(query_root)


if __name__ == "__main__":
    main()
