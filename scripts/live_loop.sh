#!/usr/bin/env bash
# Periodically batch raw Bronze data through Silver, daily features, and scoring.

cd "$(dirname "$0")/.."

stop_demo() {
  if [[ -n "${CHILD_PID:-}" ]]; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
  fi
  echo "[$(date +%H:%M:%S)] Live loop stopped."
  exit 0
}
trap stop_demo INT TERM

run_command() {
  "$@" &
  CHILD_PID=$!
  wait "$CHILD_PID"
  local status=$?
  CHILD_PID=""
  return "$status"
}

while true; do
  today="$(date +%Y-%m-%d)"
  echo "[$(date +%H:%M:%S)] Running quality gate..."
  if run_command make quality; then
    echo "[$(date +%H:%M:%S)] Quality gate completed."
  else
    status=$?
    echo "[$(date +%H:%M:%S)] Quality gate completed with exit status ${status}; continuing."
  fi

  echo "[$(date +%H:%M:%S)] Building live features for ${today}..."
  if run_command ./.venv/bin/python -m scripts.build_live_features --date "${today}"; then
    echo "[$(date +%H:%M:%S)] Running score-day for ${today}..."
    if run_command ./.venv/bin/python -m scoring.score_day --date "${today}"; then
      :
    else
      echo "[$(date +%H:%M:%S)] Scoring failed for ${today} (no features or scoring error); continuing loop."
    fi
  else
    echo "[$(date +%H:%M:%S)] No live data yet for today, skipping feature build and scoring."
  fi

  echo "[$(date +%H:%M:%S)] Cycle complete. Sleeping 60s."
  sleep 60
done
