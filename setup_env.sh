#!/usr/bin/env bash
# =============================================================================
# setup_env.sh
# =============================================================================
# Creates (or updates) the 'pom' conda environment used by POM_analysis.
#
# Usage
# -----
#   bash setup_env.sh              # create environment from scratch
#   bash setup_env.sh --update     # update existing environment
#
# After running this script, activate the environment with:
#   conda activate pom
# =============================================================================

set -euo pipefail

ENV_NAME="pom"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "======================================================"
echo "  POM_analysis – conda environment setup"
echo "  Environment name : ${ENV_NAME}"
echo "======================================================"

# --------------------------------------------------------------------------
# Check conda is available
# --------------------------------------------------------------------------
if ! command -v conda &>/dev/null; then
    echo "[ERROR] conda not found. Please install Miniconda or Anaconda first."
    echo "        https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi

# --------------------------------------------------------------------------
# Create or update
# --------------------------------------------------------------------------
if conda env list | grep -q "^${ENV_NAME} "; then
    if [[ "${1:-}" == "--update" ]]; then
        echo "[INFO] Updating existing '${ENV_NAME}' environment..."
        conda env update -n "${ENV_NAME}" \
              -f "${SCRIPT_DIR}/environment.yml" --prune
    else
        echo "[INFO] Environment '${ENV_NAME}' already exists."
        echo "       Run with --update to refresh packages."
        echo "       Activate with:  conda activate ${ENV_NAME}"
        exit 0
    fi
else
    echo "[INFO] Creating new '${ENV_NAME}' environment..."
    conda env create -n "${ENV_NAME}" \
          -f "${SCRIPT_DIR}/environment.yml"
fi

# --------------------------------------------------------------------------
# Post-install info
# --------------------------------------------------------------------------
echo ""
echo "======================================================"
echo "  Setup complete!"
echo ""
echo "  Activate the environment:"
echo "    conda activate ${ENV_NAME}"
echo ""
echo "  Verify installation:"
echo "    python -c \"from utils.grads_reader import GrADSReader; print('OK')\""
echo ""
echo "  Run the data inspection script:"
echo "    python scripts/exploratory/inspect_data.py --region south_atlantic"
echo "======================================================"
