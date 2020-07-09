#!/usr/bin/env bash
set -o xtrace

DEST_PROJ="moz-fx-data-bq-data-science"
DEST_TABLE="$DEST_PROJ:wbeard.test_slow_regression_test_data"

python gen_test_data_query.py \
    --fill_yesterday=True | \
    bq query --use_legacy_sql=false \
    --destination_table="$DEST_TABLE" \
    --replace=true \
    --project_id=moz-fx-data-derived-datasets