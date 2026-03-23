"""
explore_gesla_vs_surgemip.py
============================
Exploratory: compare station coverage between GESLA-4 ZIP archive and the
SurgeMIP station list.  Produces a CSV table with all station names and two
columns (GESLA, SURGEMIP) marked with "X" to indicate presence.

Output
------
    results/exploratory/gesla_vs_surgemip_coverage.csv

Usage
-----
    python scripts/exploratory/explore_gesla_vs_surgemip.py
"""

import pathlib
import zipfile

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT        = pathlib.Path(__file__).resolve().parents[2]
GESLA_ZIP   = ROOT / "data" / "gesla" / "raw" / "GESLA4_ALL.zip"
SURGEMIP_CSV= ROOT / "data" / "SurgeMIP_files" / "SurgeMIP_stnlist.csv"
OUT_DIR     = ROOT / "results" / "exploratory"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE    = OUT_DIR / "gesla_vs_surgemip_coverage.csv"

# ---------------------------------------------------------------------------
# 1. Station names from GESLA-4 ZIP  (stem = filename without extension)
# ---------------------------------------------------------------------------
print("Reading GESLA-4 ZIP …")
with zipfile.ZipFile(GESLA_ZIP) as zf:
    gesla_stems = {
        pathlib.Path(name).stem.lower()
        for name in zf.namelist()
        if not name.endswith("/")   # skip directory entries
    }
print(f"  {len(gesla_stems)} files in ZIP")

# ---------------------------------------------------------------------------
# 2. Station names from SurgeMIP list  (first column: FILE NAME)
# ---------------------------------------------------------------------------
print("Reading SurgeMIP station list …")
surgemip_df = pd.read_csv(SURGEMIP_CSV, usecols=[0])
surgemip_df.columns = ["file_name"]
surgemip_stems = set(surgemip_df["file_name"].str.strip().str.lower())
print(f"  {len(surgemip_stems)} stations in SurgeMIP list")

# ---------------------------------------------------------------------------
# 3. Union of all names → coverage table
# ---------------------------------------------------------------------------
all_stations = sorted(gesla_stems | surgemip_stems)

rows = []
for stn in all_stations:
    rows.append({
        "station": stn,
        "GESLA":    "X" if stn in gesla_stems    else "",
        "SURGEMIP": "X" if stn in surgemip_stems else "",
    })

table = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 4. Summary
# ---------------------------------------------------------------------------
both        = table[(table["GESLA"] == "X") & (table["SURGEMIP"] == "X")]
gesla_only  = table[(table["GESLA"] == "X") & (table["SURGEMIP"] == "")]
surge_only  = table[(table["GESLA"] == "")  & (table["SURGEMIP"] == "X")]

print(f"\n{'='*50}")
print(f"Total unique stations : {len(table)}")
print(f"  In GESLA only       : {len(gesla_only)}")
print(f"  In SurgeMIP only    : {len(surge_only)}")
print(f"  In BOTH             : {len(both)}")
print(f"{'='*50}")

# ---------------------------------------------------------------------------
# 5. Save
# ---------------------------------------------------------------------------
table.to_csv(OUT_FILE, index=False)
print(f"\nTable saved: {OUT_FILE}")
