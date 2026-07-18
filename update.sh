#!/bin/bash
# Refreshes the flood dashboard data from BigQuery and pushes it live.
# Run manually, or via the com.apa.uvalde-flood-dashboard LaunchAgent (every 15 min).

set -e

cd "$(dirname "$0")"

# Create virtual environment on first run
if [ ! -d ".venv" ]; then
  echo "Setting up Python environment (first run only)..."
  python3 -m venv .venv
  .venv/bin/pip install --quiet google-cloud-bigquery shapely
fi

echo "Fetching flood animal data from BigQuery..."
.venv/bin/python fetch_data.py

echo "Pushing to GitHub..."
git add data/flood_animals.json
git diff --staged --quiet && echo "No new data." && exit 0
git commit -m "Update flood animal data $(date +'%Y-%m-%d %H:%M')"
git push origin main

echo "Done. Dashboard will update in ~30 seconds."
