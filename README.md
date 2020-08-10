slow_regressions
==============================

# Shell aliases

```sh
alias db="docker build -t ds_546_prod ."
alias dr="docker run -v=$HOME/.config/gcloud:/root/.config/gcloud -v ~/repos/sreg:/sreg -it ds_546_prod /bin/bash"


function da () {
    export CONTAINER=`docker container ls | pcregrep -o "^[a-z0-9]+"`
    docker exec -it $CONTAINER /bin/bash
}
```



# Status

`python -m slow_regressions.utils.diagnose_etl dates`


# Ipython

```
%run slow_regressions/ipyimp.py
```

# Workflow

## etl.sh layout

- `slow_regressions.load_raw_test_data etl`
  - Downloads summary data from
        `moz-fx-data-derived-datasets.taskclusteretl.perfherder` to
        temp table
  - 
- `python -m slow_regressions.etl load_brms --brms_dir='/sreg/data/' --date="$yesterday"`
- `time Rscript slow_regressions/model/smod.r "/sreg/data/$yesterday/"`
- `python -m slow_regressions.etl upload_model_data \
      --subdate="$yesterday" --model_data_dir='/sreg/data/'`



## Upload test summaries

```python
gtd.extract_upload_test_data(bq_query=bq.bq_query2, start_date=6)
```

```sh
python -m slow_regressions.load_raw_test_data etl --start_date=0
```



```sh
bash slow_regressions/data/load_test_data.sh
```

## Download test data
```python
import slow_regressions.etl as sr
df_ = sr.load('2020-07-06')
df = sr.transform_test_data(df_)
sr.load_write_suite_ts('2020-07-06')
```

### CLI
```sh
python -m slow_regressions.etl load_brms --brms_dir='/sreg/data/' --date='2020-07-07'
```

## Run R model
```sh
cd /sreg
time Rscript slow_regressions/model/smod.r /sreg/data/2020-07-07/
```

## Post model building

### CLI

```sh
python -m slow_regressions.etl upload_model_data --subdate='2020-07-07' --model_data_dir='/sreg/data/'
```



### Python

```py

import slow_regressions.utils.model_eval as me
import slow_regressions.etl as sr
# import slow_regressions.utils.suite_utils as su
# import slow_regressions.utils.bq_utils as bq

bv = sr.load_beta_versions()
mm = me.ModelManager("/sreg/data/", "2020-07-06", bv)

# Upload input
df_inp = sr.transform_model_input_data(mm.suite_plats)
sr.upload_model_input_data(df_inp, replace=False, skip_existing=True)

# Upload samples
draws_ul = sr.transform_model_posterior(mm.suite_plats)
sr.upload_model_draws(
    draws_ul, replace=False, skip_existing=True,
)
```

# Trouble with paths
May be helpful to add this to the top of the file

```python
from pathlib import Path
import sys
project_dir = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_dir))
print(sys.path)
```



Or, call from command line with `python -m module.mod2` (but _no_ `.py`!) 