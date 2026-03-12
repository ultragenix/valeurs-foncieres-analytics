#!/bin/bash
# ══════════════════════════════════════════════════════════
# DVF Analytics — Initial Project Setup Script
# Run this from: ~/WorkSpace/valeurs-foncieres-analytics
# ══════════════════════════════════════════════════════════

set -e  # Stop on first error

echo "╔══════════════════════════════════════════════╗"
echo "║  Setting up valeurs-foncieres-analytics...   ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# Check we're in the right directory
if [ "$(basename $(pwd))" != "valeurs-foncieres-analytics" ]; then
  echo "❌ ERROR: Run this from ~/WorkSpace/valeurs-foncieres-analytics"
  echo "   Current dir: $(pwd)"
  exit 1
fi

# ── 1. Create directory structure ──
echo "📁 Creating directory structure..."
mkdir -p .claude/agents
mkdir -p terraform
mkdir -p docker/postgres
mkdir -p ingestion
mkdir -p kestra/flows
mkdir -p dbt_dvf/models/staging
mkdir -p dbt_dvf/models/intermediate
mkdir -p dbt_dvf/models/marts
mkdir -p dbt_dvf/tests
mkdir -p dbt_dvf/macros
mkdir -p dbt_dvf/seeds
mkdir -p tests/qa
mkdir -p REPORTS
mkdir -p docs
echo "  ✅ Directories created"

# ── 2. Create .gitkeep files for empty dirs ──
echo "📄 Creating .gitkeep files..."
touch REPORTS/.gitkeep
touch tests/qa/.gitkeep
touch dbt_dvf/tests/.gitkeep
touch dbt_dvf/macros/.gitkeep
touch dbt_dvf/seeds/.gitkeep
echo "  ✅ .gitkeep files created"

# ── 3. Create .gitignore ──
echo "📄 Creating .gitignore..."
cat > .gitignore << 'GITIGNORE'
# Secrets — NEVER committed
.env
*.json

# Terraform state — contains secrets in plain text
.terraform/
terraform.tfstate
terraform.tfstate.backup
*.tfvars

# Data files — too large for git
*.csv
*.sql.gz
*.sql
*.parquet
*.gpkg
data/

# Python
__pycache__/
*.pyc
.venv/
.ruff_cache/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db
GITIGNORE
echo "  ✅ .gitignore created"

# ── 4. Create .env.example ──
echo "📄 Creating .env.example..."
cat > .env.example << 'ENVEXAMPLE'
# ══════════════════════════════════════════════════════════
# DVF Analytics — Environment Variables
# Copy this file to .env and fill in your values:
#   cp .env.example .env
# ══════════════════════════════════════════════════════════

# GCP Configuration
DVF_GCP_PROJECT=valeurs-foncieres-analytics-XXXXX   # Your actual GCP project ID (check console)
DVF_GCP_REGION=europe-west9
DVF_GCP_CREDENTIALS_PATH=~/.gcp/dvf-analytics-key.json

# Google Cloud Storage
DVF_GCS_BUCKET=valeurs-foncieres-raw

# BigQuery
DVF_BQ_DATASET_RAW=dvf_raw
DVF_BQ_DATASET_STAGING=dvf_staging
DVF_BQ_DATASET_MARTS=dvf_marts

# Pipeline Mode
# demo = 1-2 departments, ~50 MB, ~10 min (default for reviewers)
# full = all France, ~5 GB, ~1-2h (for production dashboard)
DVF_MODE=demo

# PostgreSQL (ephemeral restore container — not persistent)
DVF_PG_USER=dvf
DVF_PG_PASSWORD=changeme
DVF_PG_DB=dvf_plus
DVF_PG_PORT=5432
ENVEXAMPLE
echo "  ✅ .env.example created"

# ── 5. Verify agent files exist ──
echo ""
echo "📋 Checking agent files..."
AGENTS_DIR=".claude/agents"
EXPECTED_AGENTS="architect.md briefer.md data-analyst.md developer.md documentalist.md qa-security.md tech-lead.md validator.md"
MISSING=0
for agent in $EXPECTED_AGENTS; do
  if [ -f "$AGENTS_DIR/$agent" ]; then
    echo "  ✅ $agent"
  else
    echo "  ❌ MISSING: $agent — copy it to $AGENTS_DIR/"
    MISSING=$((MISSING+1))
  fi
done

# ── 6. Verify project files exist ──
echo ""
echo "📋 Checking project files..."
FILES_TO_CHECK="CLAUDE.md AGENTS_USAGE.md docs/BRIEF.md docs/DATA_SOURCES.md"
for f in $FILES_TO_CHECK; do
  if [ -f "$f" ]; then
    echo "  ✅ $f"
  else
    echo "  ❌ MISSING: $f — copy it to the project"
    MISSING=$((MISSING+1))
  fi
done

# ── 7. Summary ──
echo ""
echo "══════════════════════════════════════════════"
if [ $MISSING -eq 0 ]; then
  echo "✅ All files in place! Ready for initial commit."
  echo ""
  echo "Next steps:"
  echo "  1. cp .env.example .env"
  echo "  2. Edit .env with your actual GCP project ID"
  echo '  3. git add .'
  echo '  4. git commit -m "chore: initial project setup with agent framework"'
  echo '  5. git push origin main'
  echo ""
  echo "Then launch Claude Code:"
  echo "  claude"
  echo "  /model   → select Opus"
  echo '  /agents architect'
  echo '  > "Read docs/BRIEF.md and generate the plan"'
else
  echo "⚠️  $MISSING file(s) missing. Copy them first, then run this script again."
fi
echo "══════════════════════════════════════════════"
