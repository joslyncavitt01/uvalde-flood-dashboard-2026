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
# Guard against a hang (e.g. a stalled network call) blocking every future run silently --
# launchd does NOT notice or restart a hung StartInterval job on its own, it just leaves it
# running forever. Confirmed to happen for real 2026-08-09: this exact command sat hung for
# 3+ days before anyone noticed the dashboard had stopped updating. No `timeout` binary on
# this Mac (not installed by default), so this is a portable bash equivalent.
FETCH_TIMEOUT=300
.venv/bin/python fetch_data.py &
FETCH_PID=$!
( sleep $FETCH_TIMEOUT && kill -9 $FETCH_PID 2>/dev/null ) &
WATCHER_PID=$!
if wait $FETCH_PID; then
  kill $WATCHER_PID 2>/dev/null
else
  kill $WATCHER_PID 2>/dev/null
  echo "fetch_data.py failed or was killed after ${FETCH_TIMEOUT}s -- skipping this run, will retry next interval."
  exit 1
fi

echo "Pushing to GitHub..."
git add data/flood_animals.json data/animal_profiles.json
git diff --staged --quiet && echo "No new data." && exit 0
git commit -m "Update flood animal data $(date +'%Y-%m-%d %H:%M')"
git push origin main

echo "Done. Dashboard will update in ~30 seconds."
