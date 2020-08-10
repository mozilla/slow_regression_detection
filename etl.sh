
yesterday=$(python -m slow_regressions.load_raw_test_data yesterday)
echo "Running for $yesterday"

# RAW_DATA_START_DATE='2020-07-07'
RAW_DATA_START_DATE=0

# Download raw taskcluster data, upload summaries
python -m slow_regressions.load_raw_test_data etl --start_date=$RAW_DATA_START_DATE --end_date="$yesterday"

# Download summarized taskcluster data, save locally
python -m slow_regressions.etl load_brms --brms_dir='/sreg/data/' --date="$yesterday"

time Rscript slow_regressions/model/smod.r "/sreg/data/$yesterday/"

python -m slow_regressions.etl upload_model_data \
    --subdate="$yesterday" --model_data_dir='/sreg/data/'