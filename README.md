slow_regressions
==============================

# Overview

At a high level, this project

* pulls data from the taskcluster `moz-fx-data-derived-datasets.taskclusteretl.perfherder` table
* arranges data by test suite and platform, performs preprocessing and outlier removal
* fits a BRMS time series model 

The primary output of this model are the MCMC samples of the BRMS model for a given day, test suite, platform combination. This allows for Bigquery calculations that, for a given test suite and platform, can make probabilistic comparisons of any day in the range.

An example usage is plotted below. 

![Output of model](https://github.com/wcbeard/slow_regression_detection/blob/master/doc/output.png)


On the right is a plot of the individual test scores over time, represented as circles. The error bands represent percentiles of the MCMC samples. 

**Note** that the test points (circles) and confidence intervals (bands) come from 2 different tables, since there isn't a clean mapping in time from one to the other. The tests (circles) can run multiple times in a day, and there can be significant gaps in time where there aren't any tests run (like in the days before/after a major release). The model samples and their intervals, however, represent the performance level for every single day in the range (the spline can automatically interpolate for days when there aren't any tests run). 

The plot on the left shows comparisons of different days in the dataset. The reference date was 'today,' and the comparison dates were 7 days ago, 14 days ago, and the last day of the 2 previous versions (version 74 and 75). Using the MCMC samples, it's possible with the query to subtract today's samples from the samples for 7 days ago, and get a probability distribution over the difference in performance between the days. A threshold can then be chosen for alerting (say, when there's an 80% chance that we're worse today than we were last week).


## The intermediate tables

It currently uses 3 tables, whose default locations are in the `tables` dict in [bq_utils.py](https://github.com/wcbeard/slow_regression_detection/blob/master/slow_regressions/utils/bq_utils.py).

The following are example usages of them

- [samples](https://sql.telemetry.mozilla.org/queries/72740/source) ([Local](doc/sample.sql))
- [input data](https://sql.telemetry.mozilla.org/queries/72659/source) ([Local](doc/input.sql))
- [compare today vs prevdays](https://sql.telemetry.mozilla.org/queries/73466/source) ([Local](doc/compare_day_performance.sql))
- [Diagnostic](doc/diagnostic.sql) (which days are filled in/missing)


# Technical notes

The model is a BRMS spline model, specified to have 2 knots per release cycle. It could probably do with fewer, but currently the `EARLY_BETA_OR_EARLIER` flag changes the test scores half way through the cycle for some tests (see [Bugzilla](https://bugzilla.mozilla.org/show_bug.cgi?id=1611809)), so 2 flags per release allows enough flexibility.

The noise model with performance data is really unpredictable 

- [This repo](https://github.com/mozilla/measure-noise) is dedicated to classifying some of the noise
- [This page](https://metrics.mozilla.com/protected/wbeard/slow_regr/distros.html) shows examples of what to expect

A spline model with Gaussian noise _usually_ describes the movements in performance data well. Problems largely arise with multimodality and outliers.

## Multimodality

Multimodality can lead to wider than usual credible intervals, but when there are multiple modes, identifying regressions isn't well defined IMO. The best I can do is flag how many modes there are with the [find_modes](https://github.com/wcbeard/slow_regression_detection/blob/master/slow_regressions/stats/modal.py#L38) function.

## Outliers

Outliers are trickier because they vary so much depending on the test. I manually filter these using a zscore that's based on number of median absolute deviations away from the running median a point falls. To the naked eye, points that fall ~4 MAD's away appear to be outliers, but some distributions have outliers that are ~100 MADs away. What's more difficult is that some distributions have an outlier rate of ~1%, but others can have up to 5%. In order to make the model general purpose enough to accommodate the potentially hundreds of different tests, the rule of thumb I went with was to [classify](https://github.com/wcbeard/slow_regression_detection/blob/f41876327bc117351ad32b007eb40352d518a6aa/slow_regressions/etl.py#L69) points more than 5 MAD's away as outliers. 

Filtering out most of the outliers this way leads to pretty nice spline fits with BRMS and Stan. The remaining problem, though, is that there are some residual outliers that can still artificially widen the credible intervals, making comparisons of performance at different points in time less meaningful. In order to correct this issue, there's currently a step that recalibrates the credible intervals so that [90%] of the data points fall within the 90% CI's.

This works because the model assumes homoscedastic noise.


### Potential future improvements 

One slick way to deal with this would be to use a Student-t noise distribution. As long as the rate of outliers is relatively small (maybe ~1% of the overall observations in a local range?) the T distribution isn't swayed by them. Here's an example comparing how a Gaussian noise model responds to outliers vs. the T distribution.



![T-dist vs Gaussian](https://github.com/wcbeard/slow_regression_detection/blob/master/doc/gaussian%20vs%20tdist.png)



I went with Gaussian + post-processing because for some test suites, Stan had a harder time fitting the T distribution. In hindsight, I'm wondering if it would work easier by getting a MLE of the `nu` (and/or the scale) parameter for a given dataset, and passing a constrained version of this as a [prior](https://rdrr.io/cran/brms/man/set_prior.html) to BRMS. It should be straightforward to get the t-distribution parameters by looking at the time series residuals `res` from, say, the rolling median, and plugging them into

```python
from stats.outliers import fwd_backwd_resid
from scipy import stats as sts

res = fwd_backwd_resid(y, ret_z=False, ret_resid=True).res_min
nu, loc, scale = sts.t.fit(res)
```


- if a model is fit on Monday, and then again on Tuesday, 



# How to run the model

```bash
git clone https://github.com/wcbeard/slow_regression_detection.git slow_reg
export SR_LOCATION="$(pwd)/slow_reg"
export GCLOUD_CREDS="$HOME/.config/gcloud"

cd $SR_LOCATION
docker build --tag ds_546_prod .
docker run -v=$GCLOUD_CREDS:/root/.config/gcloud -v $SR_LOCATION:/sreg --interactive --tty ds_546_prod

```


## Sundry notes

### Shell aliases

```sh
alias db="docker build -t ds_546_prod ."
alias dr="docker run -v=$HOME/.config/gcloud:/root/.config/gcloud -v ~/repos/sreg:/sreg -it ds_546_prod /bin/bash"


function da () {
    export CONTAINER=`docker container ls | pcregrep -o "^[a-z0-9]+"`
    docker exec -it $CONTAINER /bin/bash
}
```

### Status

`python -m slow_regressions.utils.diagnose_etl dates`


### Ipython

```
%run slow_regressions/ipyimp.py
```

### Workflow

#### etl.sh layout

- `slow_regressions.load_raw_test_data etl`
  - Downloads summary data from
        `moz-fx-data-derived-datasets.taskclusteretl.perfherder` to
        temp table
- `python -m slow_regressions.etl load_brms --brms_dir='/sreg/data/' --date="$yesterday"`
    - pulls summarized data, splits it out by suite, does some preprocessing,
    saves it to local directory that R can read from
- `time Rscript slow_regressions/model/smod.r "/sreg/data/$yesterday/"`
    - Run the R model on local data, and save MCMC samples locally
- `python -m slow_regressions.etl upload_model_data \
      --subdate="$yesterday" --model_data_dir='/sreg/data/'`
    - read MCMC samples and the preprocessed data that was fed into the model,
        upload it to BQ


### Python

```python
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
