#!/usr/bin/env bash
set -o xtrace

export PYTHONPATH="/sreg:$PYTHONPATH"
DEST_PROJ="moz-fx-data-shared-prod"
TABLE_NAME="wbeard_test_slow_regression_test_data"
DEST_TABLE="$DEST_PROJ:analysis.$TABLE_NAME"

python gen_test_data_query.py \
    --backfill=True | \
    bq query --use_legacy_sql=false \
    --destination_table="$DEST_TABLE" \
    --project_id=moz-fx-data-derived-datasets


        # --replace=true \