import scipy.stats as sts  # type: ignore
import numpy as np  # type: ignore


import pandas as pd  # type: ignore

import statsmodels.api as sm  # type: ignore
from statsmodels.robust.scale import mad  # type: ignore
import toolz.curried as z  # type: ignore

# import slow_regressions.utils.slow_reg_utils as sru

lmap = z.compose(list, map)


def robust_stats(xs):
    """
    https://en.wikipedia.org/wiki/
    Median_absolute_deviation#Relation_to_standard_deviation
    """
    scale = mad(xs) * 1.4826
    loc = np.median(xs)
    return loc, scale


def zmad(s):
    """
    MAD-based z score
    """
    s = s.values
    loc, scale = robust_stats(s)
    resid = s - loc
    return resid / scale


def robust_norm_pars(ts):
    nu, loc, scale = sts.t.fit(ts)
    return loc, scale


def robust_norm_fit(ts):
    nu, loc, scale = sts.t.fit(ts)
    return sts.norm(loc, scale)


def roll_med_resid(arr, win_len=10):
    s = pd.Series(arr).copy()
    roll_med = (
        s.rolling(win_len, center=True)
        .median()
        .fillna(method="backfill")
        .fillna(method="ffill")
    )
    resid = s - roll_med
    return resid, roll_med


def roll_med_tdist_pars(arr, win_len=10):
    """
    Use centered moving median, take residuals of that,
    and fit a t-distribution to the residuals. Return
    location and scale parameters.
    """
    resid, roll_med = roll_med_resid(arr, win_len=win_len)
    mu, sig = robust_norm_pars(resid)
    return mu, sig, roll_med


def roll_med_resid_mad(arr, win_len=10):
    resid, _ = roll_med_resid(arr, win_len=win_len)
    return mad(resid)


def min_abs(ab, b=None):
    if b is None:
        a, b = ab
    else:
        a = ab
    aa, bb = np.abs([a, b])
    if aa <= bb:
        return a
    return b


def fwd_backwd_resid(s, mad=None, win_len=10):
    """
    s must be sorted in time.
    Compute the zscore of the residuals, using both the forward and backward
    rolling medians. Then take the minimum of these 2 zscores for each
    observation. This should make it robust to level shifts,
    though mad is calculated on level-shifted medians.
    """
    assert s.is_monotonic_increasing
    df = pd.DataFrame({"s": s})
    if mad is not None:
        df["mad"] = mad

    df = _fwd_backwd_residz(df, "s", win_len=win_len)
    ret = df[["z_min"]].assign(z_mina=lambda x: x.z_min.abs())
    return ret


def _fwd_backwd_residz(df, colname, win_len=10):
    """
    Add fields to df to be able to calculate forward/backward
    residual.
    `colname` is the name of the column we want residuals of.
    Rows must be sorted.
    """
    rf = df[colname].rolling(win_len)
    rbk = df[colname][::-1].rolling(win_len)

    df["rmed_fwd"] = rf.median().fillna(method="backfill")
    df["res_fwd"] = df.eval(f"{colname} - rmed_fwd")
    sdev = sm.robust.mad(df.res_fwd)
    df["mad"] = sdev

    # df['mad_fwd'] = rf.mad().fillna(method="backfill")
    df["rmed_bkwd"] = rbk.median().fillna(method="backfill")[::-1]
    df["res_bkwd"] = df.eval(f"{colname} - rmed_bkwd")
    df["res_min"] = df[["res_bkwd", "res_fwd"]].min(axis=1)

    df["z_fwd"] = df.eval("res_fwd / mad")
    df["z_bkwd"] = df.eval("res_bkwd / mad")
    df["z_min"] = lmap(min_abs, df[["z_fwd", "z_bkwd"]].values)

    df["level_diff"] = df.eval("(res_bkwd - res_fwd) / mad")
    df["level_diffa"] = df["level_diff"].abs()

    return df

# TODO: run vulture
# def drop_outliers_zscore(y, zscore, z_thresh=4):
#     outlier = zscore.abs() > z_thresh
#     return y[~outlier]


# def residual_df(
#     sa, nstd=10, day2vers=None, time_index=True, raw_resid=False
# ):
#     if raw_resid:
#         resid = sa
#     else:
#         resid, _ = roll_med_resid(sa)
#     scale, loc = robust_stats(resid)
#     tloc, tscale = robust_norm_pars(resid)
#     time_name = sa.index.name

#     Resid = (
#         pd.DataFrame({"r": resid})
#         .assign(
#             zt=lambda x: map2deviations(x.r, tloc, tscale),
#             zrob=lambda x: map2deviations(x.r, loc, scale),
#         )
#         .assign(
#             y=sa,
#             zt_out=lambda x: x.zt.abs() > nstd,
#             zrob_out=lambda x: x.zrob.abs() > nstd,
#             l1="t_out",
#         )
#         .assign(
#             lier=lambda df: lmap(
#                 lier, df[["zt_out", "zrob_out", "l1"]].values
#             )
#         )
#         .drop(["l1"], axis=1)
#         .reset_index(drop=0)
#         .rename(columns={time_name: "t"})
#     )
#     dt = pd.to_datetime(Resid.t)
#     if time_index:
#         Resid = Resid.assign(dow=lambda x: sru.ndow(dt))
#     if day2vers is not None:
#         day = pd.to_datetime(dt.dt.date)
#         Resid = Resid.assign(vers=day.map(day2vers))
#     return Resid
