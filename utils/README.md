# Utils

Reusable Python modules shared across scripts.

---

## `grads_reader.py`

Low-level reader for GrADS sequential-binary (`.gra`) files paired with a
descriptor (`.ctl`), as produced by the Princeton Ocean Model (POM).

### Key API

| Symbol | Description |
|--------|-------------|
| `parse_ctl(ctl_path)` | Parse a GrADS CTL descriptor → `dict` |
| `GrADSReader(ctl_path)` | Memory-mapped reader (never loads the full ~138 GB file) |
| `reader.describe()` | Print dataset summary |
| `reader.read_timestep(t_idx)` | One 2-D field → `(ny, nx)` masked array |
| `reader.read_slice(t_start, t_end)` | Time range → `(nt, ny, nx)` |
| `reader.nearest_ij(lon, lat)` | Nearest grid indices `(j, i)` |
| `reader.nearest_wet_ij(lon, lat)` | Nearest **ocean** grid indices (expands search to avoid land) |
| `reader.extract_point(lon, lat, …)` | Full time series at a point → `(times, ts, grid_lon, grid_lat)` |

### Quick example

```python
from utils.grads_reader import GrADSReader
from config.settings import TIDE_CTL

r   = GrADSReader(TIDE_CTL, verbose=False)
eta = r.read_timestep(0)                     # shape (ny, nx)
times, ts, glon, glat = r.extract_point(-46.3, -23.97)
```

---

## `gesla.py`

Utilities for working with GESLA-4 tide-gauge data.

### Key API

| Symbol | Description |
|--------|-------------|
| `load_station_list(csv_path)` | Read and clean `SurgeMIP_stnlist.csv` → `pd.DataFrame` |
| `build_manifest(station_list, out_path)` | Write lean `stations_manifest.csv` |
| `parse_gesla_file(source, station_meta, …)` | Parse one GESLA station file → `pd.DataFrame` |
| `find_station_in_zip(zip_path, file_name)` | Locate a station file inside the GESLA ZIP |

### Timezone rule

`UTC = local_time − timedelta(hours=TIME_ZONE_HOURS)`

If `TIME_ZONE_HOURS == 0` the data are treated as already in UTC.
If the value is missing/NaN, UTC is assumed and logged as a warning.

### Quick example

```python
from utils.gesla import load_station_list, parse_gesla_file
from config.settings import SURGEMIP_STNLIST, GESLA_OBS_DIR

stations = load_station_list(SURGEMIP_STNLIST)
row      = stations.iloc[0].to_dict()

df = parse_gesla_file(
    GESLA_OBS_DIR / f"{row['file_name']}",
    station_meta=row,
)
print(df.head())
```

---

## Adding a new utility module

1. Create `utils/<module_name>.py` with clear docstrings and type hints.
2. Export public symbols in `utils/__init__.py` if appropriate.
3. Add a section to this README describing the module's API.
4. Update `CONTRIBUTING.md` if the module introduces new conventions.
