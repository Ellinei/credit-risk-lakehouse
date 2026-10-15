#!/usr/bin/env bash
# Smoke test of the real Glue entrypoint, run in the AWS Glue 5 container.
#
# 1. The replayer lands three daily slices of the fixture CSVs under out/smoke/landing.
# 2. bronze_to_silver.py runs with spark-submit on local paths (bookmarks off).
# 3. The job runs again in reprocess mode for a day where four tables have no files;
#    those tables must be skipped and the totals must not change.
# 4. check_silver.py asserts on the silver and quarantine output after each run.
#
# Usage (from anywhere, Git Bash or Linux):  glue/tests/smoke/run_smoke.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
IMAGE="${GLUE_IMAGE:-public.ecr.aws/glue/aws-glue-libs:5}"
PYTHON="${PYTHON:-$REPO/.venv/Scripts/python}"
[ -x "$PYTHON" ] || PYTHON="$REPO/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON=python3

# Docker Desktop on Windows wants a Windows path for the mount; pwd -W gives one in Git Bash.
MOUNT="$( (cd "$REPO" && pwd -W) 2>/dev/null || echo "$REPO")"
export MSYS_NO_PATHCONV=1

WS=/home/hadoop/workspace
OUT="$REPO/out/smoke"
rm -rf "$OUT"

cd "$REPO"
for day in 2026-10-01 2026-10-02 2026-10-03; do
  "$PYTHON" -m replayer.replay --data-dir tests/fixtures/home_credit --ingest-date "$day" \
    --start-date 2026-10-01 --clients-per-slice 3 --target local --out-dir out/smoke/landing
done

JOB_ARGS="--JOB_NAME smoke \
  --raw_root file://$WS/out/smoke/landing/raw \
  --silver_root file://$WS/out/smoke/silver \
  --quarantine_root file://$WS/out/smoke/quarantine \
  --job-bookmark-option job-bookmark-disable \
  --max_quarantine_ratio 1.0"

docker run --rm -v "$MOUNT:$WS" -w "$WS" "$IMAGE" -c "
  set -e
  export PYTHONPATH=$WS/glue:\$PYTHONPATH
  spark-submit $WS/glue/jobs/bronze_to_silver.py $JOB_ARGS
  python3 $WS/glue/tests/smoke/check_silver.py file://$WS/out/smoke
  echo '--- reprocess 2026-10-03 (four tables have no files that day) ---'
  spark-submit $WS/glue/jobs/bronze_to_silver.py $JOB_ARGS --reprocess_ingest_date 2026-10-03
  python3 $WS/glue/tests/smoke/check_silver.py file://$WS/out/smoke
"
