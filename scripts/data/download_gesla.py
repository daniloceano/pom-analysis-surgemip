"""
download_gesla.py
=================
Download the GESLA-4 dataset ZIP archive and (optionally) extract only the
station files listed in the SurgeMIP station list.

The full GESLA-4 dataset requires free registration at:
  https://gesla787883612.wordpress.com/downloads/

After registering, you receive a direct download link.  Pass that URL via
``--url`` or set the environment variable ``GESLA_ZIP_URL``.

The download is idempotent: if the ZIP already exists and ``--force`` is not
set the script skips the download and proceeds directly to extraction.

Usage
-----
    # Download the full GESLA-4 archive (replace URL with your download link):
    python scripts/data/download_gesla.py \\
        --url "https://<your-gesla-download-link>/GESLA4.zip"

    # Use a ZIP you already have locally and extract stations:
    python scripts/data/download_gesla.py \\
        --zip-file /path/to/GESLA4.zip \\
        --extract

    # Download and immediately extract all SurgeMIP stations:
    python scripts/data/download_gesla.py \\
        --url "https://…/GESLA4.zip" \\
        --extract

    # Force re-download even if the file exists:
    python scripts/data/download_gesla.py --url "…" --force

Environment variables
---------------------
    GESLA_ZIP_URL   – default download URL (alternative to --url)
    GESLA_ZIP_FILE  – default local path for the archive (alternative to --zip-file)
"""

import argparse
import logging
import pathlib
import sys
import urllib.request

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    GESLA_ZIP_URL,
    GESLA_ZIP_FILE,
    GESLA_RAW_DIR,
    SURGEMIP_STNLIST,
)
from utils.gesla import load_station_list, find_station_in_zip, _find_stem_in_namelist

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--url",
        default=GESLA_ZIP_URL or None,
        help="Direct download URL for the GESLA-4 ZIP archive.",
    )
    p.add_argument(
        "--zip-file",
        default=GESLA_ZIP_FILE,
        help=f"Local path for the downloaded archive. (default: {GESLA_ZIP_FILE})",
    )
    p.add_argument(
        "--extract",
        action="store_true",
        help="After downloading, extract station files listed in the SurgeMIP station list.",
    )
    p.add_argument(
        "--extract-dir",
        default=str(GESLA_RAW_DIR / "stations"),
        help="Directory where individual station files are extracted.",
    )
    p.add_argument(
        "--station-list",
        default=str(SURGEMIP_STNLIST),
        help="Path to SurgeMIP_stnlist.csv.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the archive already exists locally.",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def _download_with_progress(url: str, dest: pathlib.Path) -> None:
    """Download *url* to *dest*, printing a simple progress bar."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _reporthook(block_count: int, block_size: int, total_size: int) -> None:
        downloaded = block_count * block_size
        if total_size > 0:
            pct = min(100, downloaded / total_size * 100)
            bar = "#" * int(pct / 2)
            print(f"\r  [{bar:<50}] {pct:5.1f}%  {downloaded/1e6:.0f} MB", end="", flush=True)
        else:
            print(f"\r  Downloaded {downloaded/1e6:.0f} MB", end="", flush=True)

    logger.info("Downloading: %s", url)
    logger.info("         →  %s", dest)
    urllib.request.urlretrieve(url, dest, reporthook=_reporthook)
    print()  # newline after progress bar
    logger.info("Download complete. Size: %.1f MB", dest.stat().st_size / 1e6)


# ---------------------------------------------------------------------------
# Extraction helper
# ---------------------------------------------------------------------------

def extract_stations(
    zip_path: pathlib.Path,
    station_list_path: pathlib.Path,
    extract_dir: pathlib.Path,
) -> tuple[int, int, list[str]]:
    """
    Extract only the station files listed in *station_list_path* from *zip_path*.

    Returns
    -------
    (n_extracted, n_skipped, n_missing) : tuple[int, int, list[str]]
        Number of files extracted, skipped (already exist), and file names
        not found in the archive.
    """
    import zipfile

    stations = load_station_list(station_list_path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    n_extracted = 0
    n_skipped   = 0
    missing: list[str] = []

    logger.info("Opening ZIP archive: %s", zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        namelist = zf.namelist()

        for _, row in stations.iterrows():
            file_name = str(row["file_name"]).strip()
            dest_file = extract_dir / file_name

            if dest_file.exists():
                logger.debug("  SKIP (exists): %s", file_name)
                n_skipped += 1
                continue

            stem  = file_name.lower()
            match = _find_stem_in_namelist(stem, namelist)
            if match is None:
                logger.warning("  NOT FOUND in ZIP: %s", file_name)
                missing.append(file_name)
                continue

            data = zf.read(match)
            dest_file.write_bytes(data)
            n_extracted += 1
            logger.debug("  EXTRACTED: %s", file_name)

    logger.info(
        "Extraction complete — extracted=%d  skipped=%d  missing=%d",
        n_extracted, n_skipped, len(missing),
    )
    if missing:
        logger.warning("Stations not found in archive (%d):", len(missing))
        for m in missing[:20]:
            logger.warning("  %s", m)
        if len(missing) > 20:
            logger.warning("  … and %d more.", len(missing) - 20)

    return n_extracted, n_skipped, missing


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    zip_path = pathlib.Path(args.zip_file)

    # ---- Download -----------------------------------------------------------
    if zip_path.exists() and not args.force:
        logger.info("Archive already exists: %s  (use --force to re-download)", zip_path)
    elif args.url:
        _download_with_progress(args.url, zip_path)
    else:
        logger.error(
            "No URL provided and archive not found at %s.\n"
            "  • Pass --url <download_link>  or\n"
            "  • Set the GESLA_ZIP_URL environment variable  or\n"
            "  • Download manually from https://gesla787883612.wordpress.com/downloads/\n"
            "    and place the file at %s",
            zip_path, zip_path,
        )
        sys.exit(1)

    # ---- Extract ------------------------------------------------------------
    if args.extract:
        if not zip_path.exists():
            logger.error("Cannot extract — archive not found: %s", zip_path)
            sys.exit(1)

        extract_dir = pathlib.Path(args.extract_dir)
        station_list = pathlib.Path(args.station_list)

        extract_stations(zip_path, station_list, extract_dir)
    else:
        logger.info(
            "Archive ready at %s\n"
            "  Run with --extract to unpack station files.",
            zip_path,
        )


if __name__ == "__main__":
    main()
