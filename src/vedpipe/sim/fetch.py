"""Download VED and extract the first N weekly CSVs plus the static xlsx files.

Idempotent by design: the tarball is only downloaded when no archives are present,
only missing CSVs are extracted, and a manifest short-circuits a re-run with the same
``VED_WEEKS``. Archives can also be dropped into ``<data>/archives`` manually (e.g. when
GitHub serves Git LFS pointers instead of the real files).
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import tarfile
import tempfile
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any

import py7zr
import requests

from vedpipe.config import Settings, setup_logging
from vedpipe.sim.ved import DYNAMIC_DIR, MANIFEST, week_date

log = logging.getLogger(__name__)

TARBALL_URL = "https://codeload.github.com/gsoh/VED/tar.gz/refs/heads/master"
LFS_MAGIC = b"version https://git-lfs"


class FetchError(RuntimeError):
    """The dataset could not be obtained in a usable form."""


def select_weeks(names: Iterable[str], weeks: int) -> list[str]:
    """The chronologically first ``weeks`` weekly files (0 = all)."""
    dated = sorted((d, n) for n in names if (d := week_date(n)) is not None)
    chosen = dated if weeks <= 0 else dated[:weeks]
    return [name for _, name in chosen]


def ensure_not_lfs_pointer(path: Path) -> None:
    """Fail with instructions instead of a cryptic 7z error on a Git LFS pointer file."""
    if path.stat().st_size < 1024 and path.read_bytes().startswith(LFS_MAGIC):
        raise FetchError(
            f"{path.name} is a Git LFS pointer, not the archive. Download the real file "
            f"(e.g. 'git lfs pull' in a clone of github.com/gsoh/VED) and place it in "
            f"{path.parent}."
        )


def download(url: str, dest: Path, timeout_s: float = 60.0) -> None:
    """Stream to a temp file and rename, so an interrupted download never looks complete."""
    part = dest.with_suffix(dest.suffix + ".part")
    log.info("downloading %s", url)
    with requests.get(url, stream=True, timeout=timeout_s) as response:
        response.raise_for_status()
        with part.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    part.replace(dest)
    log.info("downloaded %.1f MB", dest.stat().st_size / 1e6)


def extract_data_members(tarball: Path, archive_dir: Path) -> list[Path]:
    """Copy ``*/Data/*.7z`` and ``*/Data/*.xlsx`` out of the GitHub tarball.

    Members are written under their base name only, so a crafted path inside the
    tarball cannot escape ``archive_dir``.
    """
    archive_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    with tarfile.open(tarball, "r:gz") as tar:
        for member in tar:
            path = PurePosixPath(member.name)
            if not member.isfile() or path.parent.name != "Data":
                continue
            if path.suffix.lower() not in {".7z", ".xlsx"}:
                continue
            target = archive_dir / path.name
            if not target.exists():
                source = tar.extractfile(member)
                if source is None:
                    continue
                with source, target.open("wb") as fh:
                    shutil.copyfileobj(source, fh)
            written.append(target)
    if not any(p.suffix == ".7z" for p in written):
        raise FetchError("no .7z archives found under Data/ in the VED tarball")
    return written


def _manifest_is_current(data_dir: Path, weeks: int) -> bool:
    manifest_path = data_dir / MANIFEST
    if not manifest_path.exists():
        return False
    manifest = json.loads(manifest_path.read_text())
    files = [data_dir / DYNAMIC_DIR / n for n in manifest["csv_files"]]
    files += [data_dir / n for n in manifest["static_files"]]
    return manifest.get("weeks") == weeks and all(f.exists() for f in files)


def fetch(data_dir: Path, weeks: int, url: str = TARBALL_URL) -> dict[str, Any]:
    """Make ``data_dir`` contain the static xlsx files and the selected weekly CSVs."""
    if _manifest_is_current(data_dir, weeks):
        log.info("VED data already present in %s (weeks=%d); nothing to do", data_dir, weeks)
        return json.loads((data_dir / MANIFEST).read_text())

    archive_dir = data_dir / "archives"
    csv_dir = data_dir / DYNAMIC_DIR
    csv_dir.mkdir(parents=True, exist_ok=True)

    if not list(archive_dir.glob("*.7z")):
        tarball = data_dir / "VED-master.tar.gz"
        if not tarball.exists():
            download(url, tarball)
        extract_data_members(tarball, archive_dir)
        tarball.unlink()  # the archives are kept; the 160 MB tarball is not needed again

    static_files = []
    for xlsx in sorted(archive_dir.glob("VED_Static_Data_*.xlsx")):
        ensure_not_lfs_pointer(xlsx)
        shutil.copy2(xlsx, data_dir / xlsx.name)
        static_files.append(xlsx.name)
    if not static_files:
        raise FetchError(f"no VED_Static_Data_*.xlsx in {archive_dir}")

    members: dict[str, Path] = {}
    for archive in sorted(archive_dir.glob("*.7z")):
        ensure_not_lfs_pointer(archive)
        with py7zr.SevenZipFile(archive, "r") as zf:
            for name in zf.getnames():
                if week_date(name) is not None:
                    members[name] = archive
    selected = select_weeks(members, weeks)
    if not selected:
        raise FetchError("no VED_mmddyy_week.csv files found in the archives")

    for archive in sorted(set(members.values())):
        wanted = [
            n for n in selected
            if members[n] == archive and not (csv_dir / PurePosixPath(n).name).exists()
        ]
        if not wanted:
            continue
        log.info("extracting %d weekly file(s) from %s", len(wanted), archive.name)
        with tempfile.TemporaryDirectory(dir=data_dir) as tmp, py7zr.SevenZipFile(
            archive, "r"
        ) as zf:
            zf.extract(path=tmp, targets=wanted)
            for name in wanted:
                (Path(tmp) / name).replace(csv_dir / PurePosixPath(name).name)

    manifest = {
        "source": url,
        "weeks": weeks,
        "csv_files": [PurePosixPath(n).name for n in selected],
        "static_files": static_files,
    }
    (data_dir / MANIFEST).write_text(json.dumps(manifest, indent=2))
    log.info("VED ready: %d weekly file(s), %d static file(s)", len(selected), len(static_files))
    return manifest


def main(argv: list[str] | None = None) -> None:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.ved_data_dir)
    parser.add_argument("--weeks", type=int, default=settings.ved_weeks, help="0 = all weeks")
    parser.add_argument("--url", default=TARBALL_URL)
    args = parser.parse_args(argv)
    setup_logging()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    fetch(args.data_dir, args.weeks, args.url)


if __name__ == "__main__":
    main()
