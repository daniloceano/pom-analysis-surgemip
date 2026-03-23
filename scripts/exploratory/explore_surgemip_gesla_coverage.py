"""
explore_surgemip_gesla_coverage.py
===================================
Exploratory: for every station in the SurgeMIP list, check whether its GESLA-4
data file is present in the local archive.  Produces a table to help diagnose
why some stations are missing.

Columns in output
-----------------
file_name       SurgeMIP station identifier
site_name       Human-readable name
country         Country code
contributor     Data contributor abbreviation
latitude        Decimal degrees
longitude       Decimal degrees
gauge_type      e.g. Coastal / Offshore
in_gesla        X if the station file was found in GESLA-4 ZIP (exact or fuzzy)
obs_csv_exists  X if the prepared observation CSV already exists locally
status          FOUND | FUZZY_MATCH | MISSING

Output
------
    results/exploratory/surgemip_gesla_coverage.csv

Usage
-----
    python scripts/exploratory/explore_surgemip_gesla_coverage.py
"""

import pathlib
import sys
import zipfile

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT         = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

GESLA_ZIP    = ROOT / "data" / "gesla" / "raw" / "GESLA4_ALL.zip"
SURGEMIP_CSV = ROOT / "data" / "SurgeMIP_files" / "SurgeMIP_stnlist.csv"
OBS_DIR      = ROOT / "data" / "processed" / "gesla" / "observations"
OUT_DIR      = ROOT / "results" / "exploratory"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE     = OUT_DIR / "surgemip_gesla_coverage.csv"

from utils.gesla import _find_stem_in_namelist   # reuse existing matching logic

# ---------------------------------------------------------------------------
# 1. Load SurgeMIP list
# ---------------------------------------------------------------------------
print("Loading SurgeMIP station list …")
surge = pd.read_csv(
    SURGEMIP_CSV,
    usecols=["FILE NAME", "SITE NAME", "COUNTRY",
             "CONTRIBUTOR (ABBREVIATED)", "LATITUDE", "LONGITUDE", "GAUGE TYPE"],
    dtype=str,
)
surge.columns = ["file_name", "site_name", "country",
                 "contributor", "latitude", "longitude", "gauge_type"]
surge["file_name"] = surge["file_name"].str.strip()
print(f"  {len(surge)} stations")

# ---------------------------------------------------------------------------
# 2. Load GESLA ZIP namelist
# ---------------------------------------------------------------------------
print("Reading GESLA-4 ZIP …")
with zipfile.ZipFile(GESLA_ZIP) as zf:
    zip_namelist = zf.namelist()
zip_stems_exact = {pathlib.Path(n).stem.lower() for n in zip_namelist}
print(f"  {len(zip_namelist)} files in ZIP")

# ---------------------------------------------------------------------------
# 3. Check each SurgeMIP station against ZIP and local obs CSVs
# ---------------------------------------------------------------------------
print("Checking coverage …")
rows = []
for _, row in surge.iterrows():
    stn = row["file_name"]
    stn_lower = stn.lower()

    # Check exact match in ZIP
    exact = stn_lower in zip_stems_exact

    # Check fuzzy match (underscore-normalised) via shared utility
    fuzzy_entry = None
    if not exact:
        fuzzy_entry = _find_stem_in_namelist(stn_lower, zip_namelist)

    # Check local obs CSV
    obs_path = OBS_DIR / f"{stn}.csv.gz"
    obs_exists = obs_path.exists()

    if exact:
        status = "FOUND"
    elif fuzzy_entry is not None:
        status = "FUZZY_MATCH"
    else:
        status = "MISSING"

    rows.append({
        "site_name": row["site_name"],
        "in_gesla":  "X" if (exact or fuzzy_entry) else "",
    })

table = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 4. Summary
# ---------------------------------------------------------------------------
n_found   = (table["in_gesla"] == "X").sum()
n_missing = (table["in_gesla"] == "").sum()

print(f"\n{'='*40}")
print(f"SurgeMIP total : {len(table)}")
print(f"  in GESLA     : {n_found}")
print(f"  missing      : {n_missing}")
print(f"{'='*40}")

# ---------------------------------------------------------------------------
# 5. Save
# ---------------------------------------------------------------------------
table.to_csv(OUT_FILE, index=False)
print(f"\nTable saved: {OUT_FILE}")
