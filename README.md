perf
==============================

# Ipython

```
%run slow_regressions/ipyimp.py
```

# Workflow
## Download test data
```py
import slow_regressions.slow_regression_detection_etl as sr
df_ = sr.load('2020-07-06')
df = sr.transform_test_data(df_)
sr.load_write_suite_ts('2020-07-06')
```

## Run R model
```sh
cd /sreg
Rscript slow_regressions/model/smod.r /sreg/data/
```

## Post model building
```py

import slow_regressions.utils.model_eval as me
import slow_regressions.slow_regression_detection_etl as sr
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

