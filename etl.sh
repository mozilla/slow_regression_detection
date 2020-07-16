
yesterday=$(python -m slow_regressions.load_raw_test_data yesterday)
echo "Running for $yesterday"

python -m slow_regressions.load_raw_test_data etl --start_date=0 --end_date="$yesterday"
python -m slow_regressions.etl load_brms --brms_dir='/sreg/data/' --date="$yesterday"

time Rscript slow_regressions/model/smod.r "/sreg/data/$yesterday/"

python -m slow_regressions.etl upload_model_data \
    --subdate="$yesterday" --model_data_dir='/sreg/data/'