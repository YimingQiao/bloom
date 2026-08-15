#!/usr/bin/env python3
"""Download and prepare the 6,191-query CEB Stack workload for DuckDB.

The upstream data is a PostgreSQL custom-format dump. This preparer keeps the
large source archive in the gitignored benchmark cache, restores it into a
user-owned temporary PostgreSQL cluster, and copies the database into DuckDB
through DuckDB's official postgres extension.
"""

import argparse
import getpass
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parent.parent
CACHE_ROOT = ROOT / ".bench_cache" / "ceb_stack"
DATA_DIR = ROOT / ".bench_cache" / "data"

SOURCE_PAGE = "https://rmarcus.info/stack.html"
QUERY_URL = "https://rmarcus.info/so_queries.tar.zst"
QUERY_SHA256 = "6dd6a8699046a61a365722bf28c90acbf7764a2baf206114012b9cf2c8b7b918"
QUERY_ARCHIVE = CACHE_ROOT / "downloads" / "so_queries.tar.zst"
QUERY_ROOT = CACHE_ROOT / "queries" / QUERY_SHA256 / "stack"
QUERY_MANIFEST = QUERY_ROOT.parent / "manifest.json"
EXPECTED_QUERIES = 6191

DUMP_URL = "https://www.dropbox.com/s/98u5ec6yb365913/so_pg12?dl=1"
DUMP_SIZE = 19_904_524_788
DUMP_SHA256 = "5da472e3e4352960ce8f62240f360baf55eeff5244452b3ca0b8256038cf8acd"
DUMP_PATH = CACHE_ROOT / "downloads" / "so_pg12"

POSTGRES_ROOT = CACHE_ROOT / "postgres"
POSTGRES_SOCKET = CACHE_ROOT / "postgres_socket"
POSTGRES_LOG = CACHE_ROOT / "postgres.log"
POSTGRES_PORT = 55432
POSTGRES_DATABASE = "ceb_stack"
RESTORE_MANIFEST = CACHE_ROOT / "postgres_restore.json"
RESTORE_JOBS = min(32, os.cpu_count() or 1)

DATABASE_PATH = DATA_DIR / "ceb_stack.duckdb"


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verification_path(path):
    return path.with_name(path.name + ".verified.json")


def verification_is_current(path, expected_sha256):
    marker = verification_path(path)
    if not marker.is_file():
        return False
    try:
        record = json.loads(marker.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    stat = path.stat()
    return (
        record.get("sha256") == expected_sha256
        and record.get("size") == stat.st_size
        and record.get("mtime_ns") == stat.st_mtime_ns
    )


def write_verification(path, digest):
    stat = path.stat()
    verification_path(path).write_text(
        json.dumps(
            {
                "sha256": digest,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def download(url, destination, expected_size=None, expected_sha256=None):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".download")
    if destination.is_file():
        if expected_size is not None and destination.stat().st_size != expected_size:
            raise RuntimeError(f"cached file has unexpected size: {destination}")
        if expected_sha256 and not verification_is_current(
            destination, expected_sha256
        ):
            digest = file_sha256(destination)
            if digest != expected_sha256:
                raise RuntimeError(
                    f"cached file failed checksum validation: {destination}"
                )
            write_verification(destination, digest)
        return

    offset = temporary.stat().st_size if temporary.is_file() else 0
    if expected_size is not None and offset == expected_size:
        digest = file_sha256(temporary) if expected_sha256 else None
        if expected_sha256 and digest != expected_sha256:
            raise RuntimeError(
                f"downloaded file failed checksum validation: {temporary}"
            )
        os.replace(temporary, destination)
        if digest:
            write_verification(destination, digest)
        return
    if expected_size is not None and offset > expected_size:
        raise RuntimeError(f"partial download is larger than expected: {temporary}")

    headers = {"User-Agent": "Bloom-CEB-Stack-preparer"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
        print(f"Resuming {destination.name} at byte {offset:,}", flush=True)
    else:
        print(f"Downloading {destination.name} from {url}", flush=True)

    request = urllib.request.Request(url, headers=headers)
    mode = "ab" if offset else "wb"
    with urllib.request.urlopen(request) as response:
        if offset and response.status != 206:
            raise RuntimeError(
                f"server did not honor the resume request for {destination.name}"
            )
        with temporary.open(mode) as output:
            shutil.copyfileobj(response, output, length=8 * 1024 * 1024)

    if expected_size is not None and temporary.stat().st_size != expected_size:
        raise RuntimeError(
            f"downloaded {destination.name} has size {temporary.stat().st_size:,}, "
            f"expected {expected_size:,}"
        )
    if expected_sha256:
        digest = file_sha256(temporary)
        if digest != expected_sha256:
            raise RuntimeError(
                f"downloaded {destination.name} has SHA-256 {digest}, "
                f"expected {expected_sha256}"
            )
    os.replace(temporary, destination)
    if expected_sha256:
        write_verification(destination, expected_sha256)


def queries_prepared():
    return (
        QUERY_MANIFEST.is_file()
        and sum(1 for _ in QUERY_ROOT.rglob("*.sql")) == EXPECTED_QUERIES
    )


def extract_queries():
    QUERY_ROOT.parent.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="ceb-stack-queries-", dir=QUERY_ROOT.parent.parent
    ) as temporary:
        staging = Path(temporary)
        staged_queries = staging / "stack"
        staged_queries.mkdir()
        decompressor = subprocess.Popen(
            ["zstd", "-dc", str(QUERY_ARCHIVE)],
            stdout=subprocess.PIPE,
        )
        if decompressor.stdout is None:
            raise RuntimeError("could not open zstd output")
        count = 0
        try:
            with tarfile.open(fileobj=decompressor.stdout, mode="r|") as archive:
                for member in archive:
                    if not member.isfile() or not member.name.endswith(".sql"):
                        continue
                    parts = PurePosixPath(member.name).parts
                    if len(parts) != 3 or parts[0] != "so_queries":
                        continue
                    if any(part in {"", ".", ".."} for part in parts[1:]):
                        raise RuntimeError(
                            f"unsafe path in CEB Stack archive: {member.name}"
                        )
                    source = archive.extractfile(member)
                    if source is None:
                        raise RuntimeError(f"could not extract {member.name}")
                    destination = staged_queries / parts[1] / parts[2]
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with source, destination.open("wb") as output:
                        shutil.copyfileobj(source, output)
                    count += 1
        finally:
            decompressor.stdout.close()
        if decompressor.wait() != 0:
            raise RuntimeError("zstd failed while extracting CEB Stack queries")
        if count != EXPECTED_QUERIES:
            raise RuntimeError(
                f"unexpected CEB Stack query count: {count}, "
                f"expected {EXPECTED_QUERIES}"
            )

        manifest = {
            "source": SOURCE_PAGE,
            "query_url": QUERY_URL,
            "query_archive_sha256": file_sha256(QUERY_ARCHIVE),
            "queries": count,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if QUERY_ROOT.parent.exists():
            shutil.rmtree(QUERY_ROOT.parent)
        os.replace(staging, QUERY_ROOT.parent)


def prepare_queries():
    if not queries_prepared():
        download(QUERY_URL, QUERY_ARCHIVE, expected_sha256=QUERY_SHA256)
        extract_queries()
    return QUERY_ROOT


def postgres_bin(name):
    pg_config = shutil.which("pg_config")
    if not pg_config:
        raise RuntimeError("pg_config not found; install PostgreSQL server tools")
    bindir = subprocess.check_output(
        [pg_config, "--bindir"], text=True
    ).strip()
    binary = Path(bindir) / name
    if not binary.is_file():
        raise RuntimeError(f"PostgreSQL tool not found: {binary}")
    return binary


def postgres_command(name, *args, check=True, capture_output=False):
    return subprocess.run(
        [str(postgres_bin(name)), *map(str, args)],
        text=True,
        check=check,
        capture_output=capture_output,
    )


def postgres_connection_args(database=POSTGRES_DATABASE):
    return [
        "--host",
        POSTGRES_SOCKET.resolve(),
        "--port",
        str(POSTGRES_PORT),
        "--username",
        getpass.getuser(),
        "--dbname",
        database,
    ]


def start_postgres():
    POSTGRES_SOCKET.mkdir(parents=True, exist_ok=True)
    if not (POSTGRES_ROOT / "PG_VERSION").is_file():
        postgres_command(
            "initdb",
            "--pgdata",
            POSTGRES_ROOT.resolve(),
            "--auth=trust",
            "--username",
            getpass.getuser(),
        )

    status = postgres_command(
        "pg_ctl",
        "--pgdata",
        POSTGRES_ROOT.resolve(),
        "status",
        check=False,
        capture_output=True,
    )
    if status.returncode == 0:
        return False

    postgres_command(
        "pg_ctl",
        "--pgdata",
        POSTGRES_ROOT.resolve(),
        "--log",
        POSTGRES_LOG.resolve(),
        "--options",
        f"-k {POSTGRES_SOCKET.resolve()} -p {POSTGRES_PORT}",
        "start",
    )
    return True


def stop_postgres():
    postgres_command(
        "pg_ctl",
        "--pgdata",
        POSTGRES_ROOT.resolve(),
        "--mode",
        "fast",
        "stop",
        check=False,
    )


def restore_is_current(dump_digest):
    if not RESTORE_MANIFEST.is_file():
        return False
    try:
        manifest = json.loads(RESTORE_MANIFEST.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    if manifest.get("dump_sha256") != dump_digest:
        return False
    result = postgres_command(
        "psql",
        *postgres_connection_args("postgres"),
        "--tuples-only",
        "--no-align",
        "--command",
        (
            "SELECT 1 FROM pg_catalog.pg_database "
            f"WHERE datname = {sql_string(POSTGRES_DATABASE)}"
        ),
        check=False,
        capture_output=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "1"


def restore_postgres(dump_digest):
    if restore_is_current(dump_digest):
        return

    connection = postgres_connection_args("postgres")
    postgres_command("dropdb", *connection[:-2], "--if-exists", POSTGRES_DATABASE)
    postgres_command("createdb", *connection[:-2], POSTGRES_DATABASE)
    postgres_command(
        "pg_restore",
        "--exit-on-error",
        "--no-owner",
        "--no-privileges",
        "--jobs",
        str(RESTORE_JOBS),
        *postgres_connection_args(),
        DUMP_PATH.resolve(),
    )
    RESTORE_MANIFEST.write_text(
        json.dumps(
            {
                "dump_sha256": dump_digest,
                "dump_size": DUMP_PATH.stat().st_size,
                "database": POSTGRES_DATABASE,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def postgres_tables():
    result = postgres_command(
        "psql",
        *postgres_connection_args(),
        "--tuples-only",
        "--no-align",
        "--command",
        (
            "SELECT tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'public' ORDER BY tablename"
        ),
        capture_output=True,
    )
    tables = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not tables:
        raise RuntimeError("restored CEB Stack database contains no public tables")
    for table in tables:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise RuntimeError(f"unexpected PostgreSQL table name: {table!r}")
    return tables


def build_duckdb(duckdb, database=DATABASE_PATH):
    database = Path(database).resolve()
    if database.is_file():
        return
    if not duckdb.is_file():
        raise RuntimeError(f"DuckDB CLI not found: {duckdb}")

    database.parent.mkdir(parents=True, exist_ok=True)
    temporary = database.with_suffix(database.suffix + ".building")
    temporary.unlink(missing_ok=True)
    connection = (
        f"host={POSTGRES_SOCKET.resolve()} port={POSTGRES_PORT} "
        f"user={getpass.getuser()} dbname={POSTGRES_DATABASE}"
    )
    copy_tables = "\n".join(
        f'CREATE TABLE destination.public."{table}" '
        f'AS SELECT * FROM source.public."{table}";'
        for table in postgres_tables()
    )
    sql = f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH {sql_string(temporary)} AS destination;
        ATTACH {sql_string(connection)} AS source (TYPE POSTGRES, READ_ONLY);
        CREATE SCHEMA destination.public;
        {copy_tables}
        CHECKPOINT;
    """
    try:
        result = subprocess.run(
            [str(duckdb), "-c", sql],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"CEB Stack DuckDB build failed:\n{result.stderr}")
        os.replace(temporary, database)
    finally:
        temporary.unlink(missing_ok=True)


def prepare(duckdb=None, build_db=True, database=DATABASE_PATH):
    query_root = prepare_queries()
    if not build_db:
        print(f"CEB Stack queries prepared: {EXPECTED_QUERIES}")
        return query_root

    download(
        DUMP_URL,
        DUMP_PATH,
        expected_size=DUMP_SIZE,
        expected_sha256=DUMP_SHA256,
    )
    dump_digest = DUMP_SHA256 or file_sha256(DUMP_PATH)
    started_postgres = start_postgres()
    try:
        restore_postgres(dump_digest)
        duckdb = Path(duckdb or ROOT / "build" / "release" / "duckdb").resolve()
        build_duckdb(duckdb, database)
    finally:
        if started_postgres:
            stop_postgres()
    print(
        f"CEB Stack prepared: queries={EXPECTED_QUERIES}, "
        f"database={Path(database).resolve()}"
    )
    return query_root


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
