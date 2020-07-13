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


# Current canonical query:

```sql
/*
Filters Based on  Performance: Release Criteria Master
https://sql.telemetry.mozilla.org/queries/64399/source
*/

-- Parse the operating system name from the platform string
CREATE TEMP FUNCTION parseOS (platform STRING) AS (
    CASE
        WHEN STARTS_WITH(platform, "linux") THEN "linux"
        WHEN STARTS_WITH(platform, "macosx") THEN "osx"
        WHEN STARTS_WITH(platform, "windows7") THEN "windows7"
        WHEN STARTS_WITH(platform, "windows10") THEN "windows10"
        WHEN STARTS_WITH(platform, "android") THEN "android"
        ELSE platform
    END
);

with base as (
SELECT
  date(perf.time) as date,
  perf.framework,
  perf.platform,
  perf.time,
  perf.project,
  parseOS(perf.platform) as os,
  s.value as agg_val,
  s.name as sname,
  st.name as stname,
  st.value as val,  -- loadtime
FROM
  taskclusteretl.perfherder AS perf
  , UNNEST(perf.suites) AS s,
  UNNest(S.subtests) as st
WHERE
  date(time) between "2020-03-08" and "2020-03-15"
  and s.name = 'tabswitch'
  and project = 'mozilla-central'
  and perf.platform in ('windows10-64-shippable')
)


select
  *
--   project,
--   count(*) as n,
from base
-- group by 1
-- order by platform asc
```


# TaskId and group
- Each taskGroupId has multiple taskId's
- each taskId has a unique time, taskGroupId
```py
tgrp_dd = dfbaadd[["taskId", "taskGroupId", "time"]].drop_duplicates()
assert tgrp_dd.groupby(["taskId"])[["taskGroupId", "time"]].nunique().eq(1).all().all()
assert (
    tgrp_dd.groupby(["taskGroupId"])[["taskId", "time"]]
    .nunique()
    .pipe(lambda x: x.iloc[:, 0] == x.iloc[:, 1])
    .all()
)
```


# Templates
- rmarkdown: kde_modes.rmd


# Altair from rmarkdown
```r
library(rjson)
library(vegawidget)
x=fromJSON(file="~/tmp/win10_beta.json")
as_vegaspec(x)
```