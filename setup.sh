#!/bin/bash
# =============================================================================
# AirPulse — Local Setup Script (no Docker)
# Run once: bash setup.sh
# =============================================================================

set -e  # exit immediately if any command fails

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[setup]${NC} $1"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $1"; }

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
AIRFLOW_HOME="$PROJECT_DIR/.airflow"

# ── 1. Homebrew ───────────────────────────────────────────────────────────────
log "Checking Homebrew..."
if ! command -v brew &>/dev/null; then
  log "Installing Homebrew..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
  log "Homebrew already installed ✓"
fi

# ── 2. PostgreSQL ─────────────────────────────────────────────────────────────
log "Checking PostgreSQL..."
if ! command -v psql &>/dev/null; then
  log "Installing PostgreSQL 15 via Homebrew..."
  brew install postgresql@15
  brew link postgresql@15 --force
else
  log "PostgreSQL already installed ✓"
fi

log "Starting PostgreSQL service..."
brew services start postgresql@15

sleep 3  # give it a moment to start

log "Creating database 'airquality'..."
createdb airquality 2>/dev/null || warn "Database 'airquality' already exists — skipping"

log "Running DB schema init..."
psql airquality -f "$PROJECT_DIR/scripts/init_db.sql"

# ── 3. Python venv ────────────────────────────────────────────────────────────
log "Checking Python..."
PYTHON=$(command -v python3.11 || command -v python3)
if [ -z "$PYTHON" ]; then
  log "Installing Python 3.11 via Homebrew..."
  brew install python@3.11
  PYTHON=$(brew --prefix python@3.11)/bin/python3.11
fi

log "Using Python: $($PYTHON --version)"

log "Creating virtual environment at .venv ..."
$PYTHON -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

pip install --upgrade pip --quiet

# ── 4. pip packages (ingestion, dbt, FastAPI, tests) ────────────────────────
log "Installing dependencies from requirements.txt..."
pip install -r "$PROJECT_DIR/requirements.txt" --quiet

# ── 5. Apache Airflow ─────────────────────────────────────────────────────────
log "Installing Apache Airflow 2.8.0 (this takes 2-3 minutes)..."
AIRFLOW_VERSION=2.8.0
PYTHON_VERSION="$($PYTHON --version | cut -d ' ' -f2 | cut -d '.' -f1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "${CONSTRAINT_URL}" \
  --quiet

# ── 6. Airflow init ───────────────────────────────────────────────────────────
log "Initialising Airflow (home: $AIRFLOW_HOME)..."
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com 2>/dev/null || warn "Airflow admin user already exists"

# ── 7. .env file ──────────────────────────────────────────────────────────────
if [ ! -f "$PROJECT_DIR/.env" ]; then
  cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
  warn ".env created from .env.example — open it and add your WAQI_TOKEN (https://aqicn.org/data-platform/token/)!"
else
  log ".env already exists ✓"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}=====================================================${NC}"
echo -e "${GREEN}  Setup complete! Here's how to run the project:${NC}"
echo -e "${GREEN}=====================================================${NC}"
echo ""
echo "  1. Add your WAQI_TOKEN to .env (free: https://aqicn.org/data-platform/token/)"
echo ""
echo "  2. Activate the venv:"
echo "     source .venv/bin/activate"
echo ""
echo "  3. Run the pipeline once (ingest -> validate -> dbt run -> dbt test):"
echo "     python run_pipeline.py"
echo ""
echo "  4. Start the API and open the dashboard:"
echo "     uvicorn api.main:app --reload --port 8000   # http://localhost:8000"
echo ""
