#!/usr/bin/env python3
"""Package DuckDB CI artifacts into release assets and a custom repository."""

import argparse
import gzip
import hashlib
import json
import re
import shutil
from pathlib import Path

EXTENSION_NAME = "bloom"
VERSION = re.compile(r"^\d+(?:\.\d+)+$")
BUILD_REF = re.compile(r"^[0-9a-f]{40}$")
REVISION = re.compile(r"^(?:[0-9a-f]{10}|v\d+\.\d+\.\d+)$")
PLATFORM = re.compile(r"^[a-z0-9_]+$")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--extension-version", required=True)
    parser.add_argument(
        "--duckdb-revision",
        required=True,
        help="DuckDB repository revision (stable tag or 10-character source ID)",
    )
    parser.add_argument(
        "--duckdb-build-ref",
        required=True,
        help="DuckDB tag or commit used by extension-ci-tools",
    )
    parser.add_argument("--artifact-prefix", required=True)
    parser.add_argument("--artifact-postfix", default="")
    parser.add_argument(
        "--expected-platforms",
        required=True,
        help="Comma-separated DuckDB platform names",
    )
    return parser.parse_args()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def gzip_file(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as input_file, target.open("wb") as raw_output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_output, mtime=0) as output:
            shutil.copyfileobj(input_file, output)


def platform_from_artifact(path, prefix, postfix):
    artifact_name = path.parent.name
    if not artifact_name.startswith(prefix):
        raise ValueError(f"unexpected artifact directory: {artifact_name}")
    platform = artifact_name[len(prefix) :]
    if postfix:
        if not platform.endswith(postfix):
            raise ValueError(f"artifact is missing postfix {postfix!r}: {artifact_name}")
        platform = platform[: -len(postfix)]
    if not platform:
        raise ValueError(f"could not determine platform from {artifact_name}")
    return platform


def main():
    args = parse_args()
    expected = set(filter(None, args.expected_platforms.split(",")))
    if not VERSION.fullmatch(args.extension_version):
        raise SystemExit("extension version must contain dot-separated numbers")
    if not BUILD_REF.fullmatch(args.duckdb_build_ref):
        raise SystemExit("DuckDB build ref must be a full 40-character commit")
    if not REVISION.fullmatch(args.duckdb_revision):
        raise SystemExit(
            "DuckDB revision must be a stable vX.Y.Z tag or 10-character source ID"
        )
    if not expected or any(not PLATFORM.fullmatch(item) for item in expected):
        raise SystemExit("expected platforms contain an invalid platform name")

    output_dir = args.output_dir.resolve()
    repository_dir = output_dir / "repository" / args.duckdb_revision
    release_dir = output_dir / "release-assets"

    artifacts = {}
    for extension in sorted(
        args.input_dir.rglob(f"{EXTENSION_NAME}.duckdb_extension")
    ):
        platform = platform_from_artifact(
            extension, args.artifact_prefix, args.artifact_postfix
        )
        if platform in artifacts:
            raise SystemExit(f"duplicate artifact for {platform}")
        artifacts[platform] = extension

    found = set(artifacts)
    if found != expected:
        missing = sorted(expected - found)
        unexpected = sorted(found - expected)
        raise SystemExit(
            f"platform mismatch: missing={missing or 'none'}, "
            f"unexpected={unexpected or 'none'}"
        )

    manifest = {
        "extension": EXTENSION_NAME,
        "extension_version": args.extension_version,
        "duckdb_build_ref": args.duckdb_build_ref,
        "duckdb_revision": args.duckdb_revision,
        "platforms": {},
    }
    checksum_entries = []
    for platform, extension in sorted(artifacts.items()):
        repository_asset = (
            repository_dir
            / platform
            / f"{EXTENSION_NAME}.duckdb_extension.gz"
        )
        gzip_file(extension, repository_asset)

        release_name = (
            f"{EXTENSION_NAME}-{args.extension_version}-"
            f"duckdb-{args.duckdb_revision}-"
            f"{platform}.duckdb_extension.gz"
        )
        release_asset = release_dir / release_name
        release_asset.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repository_asset, release_asset)
        checksum = sha256(release_asset)
        checksum_entries.append(f"{checksum}  {release_name}")
        manifest["platforms"][platform] = {
            "asset": release_name,
            "sha256": checksum,
            "size_bytes": release_asset.stat().st_size,
        }

    checksum_path = release_dir / "SHA256SUMS"
    checksum_path.write_text("\n".join(checksum_entries) + "\n", encoding="utf-8")
    manifest_path = release_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    print(f"packaged {len(artifacts)} platforms in {output_dir}")
    print(f"release assets: {release_dir}")
    print(f"custom repository: {output_dir / 'repository'}")


if __name__ == "__main__":
    main()
