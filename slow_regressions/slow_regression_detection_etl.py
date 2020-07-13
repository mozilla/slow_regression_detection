from pathlib import Path
import tempfile
from typing import List

from fire import Fire  # type: ignore
import pandas as pd  # type: ignore

import slow_regressions.utils.bq_utils as bq
from slow_regressions.utils import suite_utils as su
import slow_regressions.utils.beta_versions as bv
import slow_regressions.utils.model_eval as me


here = Path(__file__).parent
SUBDATE = str  # should be of format "%Y-%m-%d"
default_bq_test_loc = bq.BqLocation(
    "wbeard_test_slow_regression_test_data",
    dataset="analysis",
    project_id="moz-fx-data-shared-prod",
)


def load_beta_versions():
    beta_fn = here / "data/daily_current_beta_version.sql"
    with open(beta_fn, "r") as fp:
        sql = fp.read()
    df = bq.bq_query(sql)
    assert df.day.is_monotonic_increasing

    df = df.assign(
        dw=lambda x: x.day.dt.day_name().str[:3],
        betav=lambda x: x.bvers.map(bv.get_bnum),
        mvers=lambda df: df.mvers.cummax(),
    )
    return df


def load(date: SUBDATE, bq_loc=default_bq_test_loc, history_days=365):
    assert bq.is_subdate(date)
    date_start = bq.subdate_diff(date, days=history_days)
    q = f"""
    CREATE TEMP FUNCTION major_vers(st string) AS (
      -- '10.0' => 10
      cast(regexp_extract(st, '(\\\\d+)\\\\.?') as int64)
    );

    select
      *,
      major_vers(bvers) as mvers
    from {bq_loc.sql}
    where
      date(time) between '{date_start}' and '{date}'
    """
    return bq.bq_query(q)


def transform_test_data(df):
    df = df.assign(d=lambda x: x.time.dt.date)
    return df


def write_to_brms(df, suite_dir):
    outdir = Path(suite_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    for (sname, platform), gdf in df.groupby(["sname", "platform"]):
        su.ts_write(
            sname,
            platform,
            df=gdf,
            max_daily_tests=4,
            outlier_zscore=5,
            outdir=outdir,
        )
    return outdir


def load_write_suite_ts(
    date,
    bq_loc=default_bq_test_loc,
    history_days=365,
    brms_dir="/tmp/brms",
    # suite_dir="/tmp/brms/suite_data",
):
    brms_dir = Path(brms_dir)
    suite_dir = brms_dir / "suite_data"
    (brms_dir / "brpi").mkdir(parents=True, exist_ok=True)
    (brms_dir / "br_draws").mkdir(parents=True, exist_ok=True)

    df = load(date, bq_loc=bq_loc, history_days=history_days)
    df = transform_test_data(df)
    write_to_brms(df, suite_dir=suite_dir)


def transform_model_input_data(suite_plats: List[me.SuitePlat]):
    norm_time = lambda ts: ts.dt.tz_localize(None).dt.round("ms")
    data_input_upload = (
        pd.concat(
            [
                spi.data_inp.assign(
                    suite=spi.suite,
                    platform=spi.platform.replace("_", "-"),
                )
                for spi in suite_plats
            ],
            axis=0,
            ignore_index=True,
        )
        .rename(columns={"d": "date"})
        .assign(time=lambda df: norm_time(df.time))
    )

    return data_input_upload


def transform_model_posterior_suite_plat(dr, suite, platform):
    # dr = spi.draws[:100]
    dr.columns.name = "date"
    dr.index.name = "index"
    tall = (
        dr.stack()
        .reset_index(drop=0)
        .rename(columns={0: "y"})
        .assign(suite=suite, platform=platform.replace("_", "-"))
    )
    return tall


def transform_model_posterior(suite_plats: List[me.SuitePlat]):
    data_input_upload = pd.concat(
        [
            transform_model_posterior_suite_plat(
                spi.draws, spi.suite, spi.platform
            )
            for spi in suite_plats
        ],
        axis=0,
        ignore_index=True,
    )
    return data_input_upload


def upload_model_input_data(
    data_inp,
    bql=bq.BqLocation(
        "wbeard_slow_regression_input_data_test",
        dataset="analysis",
        project_id="moz-fx-data-shared-prod",
    ),
    replace=False,
    skip_existing=False,
):
    """
    df_inp = sr.transform_model_input_data(mm.suite_plats)
    sr.upload_model_input_data(df_inp)
    """
    if skip_existing:
        existing_dates = (
            bq.pull_existing_dates(bql).map(bq.to_subdate).pipe(set)
        )
        data_inp = data_inp.pipe(
            lambda df: df[~df.date.isin(existing_dates)]
        )
        if not len(data_inp):
            print("Nothing new to upload")
            return
    bq.upload_cli(
        data_inp,
        bql,
        time_partition="date",
        add_schema=True,
        replace=replace,
    )


def upload_model_draws(
    draws,
    bql=bq.BqLocation(
        "wbeard_slow_regression_draws_test",
        dataset="analysis",
        project_id="moz-fx-data-shared-prod",
    ),
    replace=False,
    skip_existing=False,
):
    """
    draws_ul = sr.transform_model_posterior(mm.suite_plats)
    bq.upload_cli(draws_ul, bql, time_partition='date', add_schema=True,
        replace=True)
    """
    if skip_existing:
        existing_dates = (
            bq.pull_existing_dates(bql).map(bq.to_subdate).pipe(set)
        )
        draws = draws.pipe(
            lambda df: df[~df.date.isin(existing_dates)]
        )
        if not len(draws):
            print("Nothing new to upload")
            return
    bq.upload_cli(
        draws,
        bql,
        time_partition="date",
        add_schema=True,
        replace=replace,
    )


def main():
    bv = load_beta_versions()
    mm = me.ModelManager("/sreg/data/", "2020-07-06", bv)

    # Upload input
    df_inp = transform_model_input_data(mm.suite_plats)
    upload_model_input_data(
        df_inp, replace=False, skip_existing=True
    )

    # Upload samples
    draws_ul = transform_model_posterior(mm.suite_plats)
    upload_model_draws(draws_ul, replace=False, skip_existing=True)


if __name__ == "__main__":
    # currently runs from '/sreg'
    """
    python load_write_suite_ts date='2020-07-07'
    """
    Fire(
        {
            "load": load_write_suite_ts,
            "main": main,
            # "upload_model_data": upload_model_data,
        }
    )
