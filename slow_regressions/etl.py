from pathlib import Path

# import tempfile
from typing import List

from fire import Fire  # type: ignore
import pandas as pd  # type: ignore

from slow_regressions import app_dir, project_dir
import slow_regressions.utils.bq_utils as bq
from slow_regressions.utils import suite_utils as su
import slow_regressions.utils.beta_versions as bv
import slow_regressions.utils.model_eval as me

import pysnooper

SUBDATE = str  # should be of format "%Y-%m-%d"


def load_beta_versions():
    beta_fn = app_dir / "data/daily_current_beta_version.sql"
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


def load_test_data(
    date: SUBDATE, bq_loc=bq.bq_locs["test"], history_days=365
):
    print(f"date == {date}")
    assert bq.is_subdate(date)
    date_start = bq.subdate_diff(date, days=history_days)
    q = f"""
    CREATE TEMP FUNCTION major_vers(st string) AS (
      -- '10.0' => 10
      cast(regexp_extract(st, '(\\\\d+)\\\\.?') as int64)
    );

    select
      *
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


def download_write_brms(
    date,
    bq_loc=bq.bq_locs["test"],
    history_days=365,
    brms_dir=project_dir / "data",
):
    assert bq.is_subdate(date)
    brms_dir = Path(brms_dir) / date
    suite_dir = brms_dir / "suite_data"

    suite_dir.mkdir(parents=True, exist_ok=True)
    (brms_dir / "brpi").mkdir(parents=True, exist_ok=True)
    (brms_dir / "br_draws").mkdir(parents=True, exist_ok=True)

    df = load_test_data(date, bq_loc=bq_loc, history_days=history_days)
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
    bq_loc=bq.bq_locs.input_data,
    replace=False,
    skip_existing=False,
):
    """
    df_inp = sr.transform_model_input_data(mm.suite_plats)
    sr.upload_model_input_data(df_inp)
    """
    if skip_existing:
        data_inp = bq.filter_existing_dates(
            data_inp, date_col="date", bq_loc=bq_loc
        )
        if data_inp is None:
            return
    bq.upload_cli(
        data_inp,
        bq_loc,
        time_partition="date",
        add_schema=True,
        replace=replace,
    )


def upload_model_samples(
    draws,
    bq_loc=bq.bq_locs.samples,
    replace=False,
    skip_existing=False,
):
    """
    draws_ul = sr.transform_model_posterior(mm.suite_plats)
    bq.upload_cli(draws_ul, bq_loc, time_partition='date', add_schema=True,
        replace=True)
    """
    if skip_existing:
        draws = bq.filter_existing_dates(
            draws, date_col="date", bq_loc=bq_loc
        )
        if draws is None:
            return
    bq.upload_cli(
        draws,
        bq_loc,
        time_partition="date",
        add_schema=True,
        replace=replace,
    )


def upload_model_data(subdate: str, model_data_dir="/sreg/data/"):
    bv = load_beta_versions()
    subdate_data_dir = Path(model_data_dir) / subdate
    mm = me.ModelManager(subdate_data_dir, subdate, bv)

    # Upload input
    df_inp = transform_model_input_data(mm.suite_plats)
    upload_model_input_data(df_inp, replace=False, skip_existing=True)

    # Upload samples
    draws_ul = transform_model_posterior(mm.suite_plats)
    upload_model_samples(draws_ul, replace=False, skip_existing=True)


if __name__ == "__main__":
    # currently runs from '/sreg'
    """
    python -m slow_regressions.etl \
        load_brms --date='2020-07-07'
    """
    Fire(
        {
            "load_brms": download_write_brms,
            "upload_model_data": upload_model_data,
        }
    )
