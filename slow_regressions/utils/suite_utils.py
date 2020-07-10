from pathlib import Path

# import re
# import toolz.curried as z  # type: ignore

# import numpy as np  # type: ignore
# import pandas as pd  # type: ignore

import slow_regressions.utils.slow_reg_utils as sru
import slow_regressions.stats.outliers as out


@sru.requires_cols(["platform", "sname", "d", "y"])
def get_daily_row_num(df):
    """
    For each platform/suite, convert the date to an int
    where 0 is the earliest date.
    """
    cs = ["platform", "sname", "d"]
    return df.groupby(cs).y.transform(lambda x: range(len(x)))


def day2int(time):
    dates = time.dt.date
    min_date = dates.min()
    diff = dates - min_date
    return diff.astype("timedelta64[D]").astype(int)


@sru.requires_cols(["sname", "platform", "time", "d", "y"])
def transform_suite(df, max_daily_tests, outlier_zscore):
    ts = (
        df.pipe(
            lambda df: df[get_daily_row_num(df) <= max_daily_tests]
        )
        .drop(["sname", "platform"], axis=1)
        .sort_values(["time"], ascending=True)
        .assign(
            dayi=lambda df: day2int(df.time),
            z_min_abs=lambda df: out.fwd_backwd_resid(df).z_min.abs(),
            # rstd=lambda df: roll_stdev(df.y),
        )
        .assign(out=lambda x: x.z_min_abs > outlier_zscore)
        .reset_index(drop=1)
    )
    return ts


def ts_fname(sname, platform):
    return f"ts-{sname}__{platform.replace('-', '_')}.fth"


@sru.requires_cols_kw(df=["sname", "platform", "time", "d", "y"])
def ts_write(
    sname,
    platform,
    df,
    max_daily_tests=4,
    outlier_zscore=5,
    outdir="../data/interim/br/",
):
    """

    """
    r_dir = Path(outdir)
    ts = transform_suite(
        df.query("sname == @sname & platform == @platform"),
        max_daily_tests,
        outlier_zscore,
    )

    fn = r_dir / ts_fname(sname, platform)
    ts.to_feather(fn)


# def show(df):
#     show.df = df
#     print(df)
#     return df
