#!/usr/bin/env bash
set -o xtrace

# DEST_PROJ="moz-fx-data-bq-data-science"
DEST_PROJ="moz-fx-data-shared-prod"
TABLE_NAME="wbeard_test_slow_regression_test_data"
DEST_TABLE="$DEST_PROJ:analysis.$TABLE_NAME"

python gen_test_data_query.py \
    --backfill=True | \
    bq query --use_legacy_sql=false \
    --destination_table="$DEST_TABLE" \
    --replace=true \
    --project_id=moz-fx-data-derived-datasets