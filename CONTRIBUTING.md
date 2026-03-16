# Contributing to POM_analysis

Thank you for contributing!  This is a research code repository — clarity,
reproducibility, and correctness matter more than style.

---

## Code organisation

| Directory | Purpose |
|-----------|---------|
| `config/settings.py` | **Single source of truth** for all paths, constants, and settings |
| `utils/` | Reusable modules with no side-effects on import |
| `scripts/` | Executable scripts (always use `argparse`, add a module-level docstring) |
| `notebooks/` | Exploratory analysis only — not part of the production pipeline |
| `docs/` | Design decisions, assumptions, and long-form notes |

---

## Documentation rule — **mandatory**

> **Every functional change must be accompanied by a documentation update.**

Specifically:

- Any change to a **CLI interface** (new argument, renamed argument, removed
  argument) → update the script's docstring **and** `scripts/README.md`.
- Any change to an **output format** (new column, renamed column, changed
  file path) → update `data/README.md` and the relevant script docstring.
- Any change to a **utility module API** → update `utils/README.md`.
- Any new **configuration symbol** in `config/settings.py` → add it to the
  table in `README.md`.
- Any new **assumption or design decision** → add it to `docs/decisions.md`.

If you are unsure which doc to update, update `docs/decisions.md` and leave
a note explaining the change.

---

## Data policy

- **Never commit raw data, large binaries, or processed outputs.**
  Everything under `data/processed/`, `data/gesla/`, `figures/`, and
  `results/` is excluded by `.gitignore`.
- The only data file that IS versioned is `data/SurgeMIP_files/SurgeMIP_stnlist.csv`.
- Do not add hardcoded absolute paths (e.g. `/home/username/…`) to any
  versioned file.  Use `config/settings.py` and `pathlib.Path` instead.

---

## Code style

- Python 3.10+
- Type hints on all public functions.
- Module-level docstrings on every script and utility module.
- Small, single-purpose functions with clear docstrings.
- Use `logging` (not `print`) in library code; `print` is acceptable in
  interactive scripts but prefer `logging` for pipelines.
- Scripts must use `argparse` and include usage examples in their docstrings.

---

## Commit checklist

Before committing, confirm:

- [ ] All paths go through `config/settings.py` (no hardcoded absolute paths).
- [ ] No large data files are staged (`git status` shows only code/docs).
- [ ] Docstrings and relevant READMEs are updated.
- [ ] `docs/decisions.md` is updated if a new assumption was introduced.
- [ ] The script's CLI examples in the docstring still match the actual
      `argparse` interface.
- [ ] Imports work from the project root:
      `python -c "from utils.gesla import load_station_list; print('OK')"`.
