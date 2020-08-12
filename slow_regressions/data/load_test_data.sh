#!/usr/bin/env bash
set -o xtrace

DEST_PROJ="moz-fx-data-shared-prod"
DEST_TABLE="$DEST_PROJ:analysis.wbeard_test_slow_regression_test_data"

python gen_test_data_query.py \
    --fill_yesterday=True | \
    bq query --use_legacy_sql=false \
    --destination_table="$DEST_TABLE" \
    --project_id=moz-fx-data-derived-datasets

    # --replace=true \