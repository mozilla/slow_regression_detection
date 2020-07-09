import numpy as np  # type: ignore
import numpy.random as nr  # type: ignore
import pandas as pd  # type: ignore
import scipy.stats as sts  # type: ignore
import statsmodels.api as sm  # type: ignore
import toolz.curried as z  # type: ignore


def find_modes_(kde, ret_ix=True, pct_thresh=.5):
    """
    Find mode of kde by looking at where the 1st
    derivative has just changed, at the locations where
    it is negative (it just turned downward).

    pct_thresh=.5 means after finding the highest mode
    (according to the pdf), any other mode neads to be at
    least half as high as the peak.
    """
    y = kde.density
    d1 = np.gradient(y)
    s1 = np.sign(d1)
    sign_change = s1[:-1] != s1[1:]
    pct_val = y[1:] / np.max(y)
    peak_ixs = np.arange(1, len(s1))[
        sign_change & (s1[1:] < 0) & (pct_val > pct_thresh)
    ]
    if ret_ix:
        return peak_ixs
    return kde.support[peak_ixs]


def mk_kde(s):
    kde = sm.nonparametric.KDEUnivariate(s)
    kde.fit()
    return kde


def find_modes(arr, n_modes=False, pct_thresh=.5):
    kde = mk_kde(arr)
    modes = find_modes_(kde, ret_ix=False, pct_thresh=pct_thresh)
    if n_modes:
        return len(modes)
    return modes


def get_closest_mode(y, ixs):
    """
    After getting the mode indices `ixs` from kde estimation,
    return which mode each point in y is closest to.

    y: [float] of length N
    ixs: [float] of length k
    returns [0..k] of length N
    """
    broadcast_ixs = np.tile(ixs, len(y)).reshape(-1, len(ixs))
    dists = np.subtract(broadcast_ixs, y[:, None])
    abs_dists = np.abs(dists)
    closest_mode = abs_dists.argmin(axis=1)
    return closest_mode


def choose_rand(a, n):
    """
    After aggregating, randomly choose n
    rows.
    """
    N = len(a)
    if N <= n:
        return [True] * N
    extra = max(N - n, 0)
    bm = [True] * n + [False] * extra

    nr.shuffle(bm)
    return bm


def test_choose_rand():
    assert choose_rand([1, 2, 3], 3) == [True, True, True]
    assert choose_rand([1, 2, 3], 4) == [True, True, True]
    res = np.array(choose_rand([1, 2, 3, 4, 5], 2))
    assert len(res) == 5
    assert res.sum() == 2


def n_modes(s):
    if s.nunique() <= 5:
        return 1
    return find_modes(s, n_modes=True)


def geo_mean(x):
    return sts.gmean(x + 1)


# Put
def find_mode_locs(y):
    kde = mk_kde(y)
    ixs = find_modes_(kde, pct_thresh=0.1, ret_ix=False)
    return ixs


def check_series_modality(y):
    ixs = find_mode_locs(y)
    closest_mode = get_closest_mode(y, ixs)
    vc = pd.Series(closest_mode).value_counts(normalize=1)
    pct_summary = [
        f"{v:.0%} in mode {i}" for i, v in enumerate(vc, start=1)
    ]

    summary = [f"{len(ixs)} modes found"] + pct_summary
    return "\n".join(summary)


#######
# Viz #
#######
def pl_suite_modes(pdf, sname, A):
    color = "n_modes"
    x = "date"
    y = "agg_val"
    h = (
        A.Chart(pdf)
        .mark_point()
        .encode(
            x=A.X(x, title=x),
            y=A.Y(y, title="geomean", scale=A.Scale(zero=False)),
            color="n_modes:N",
            tooltip=[color, x, y],
        )
        .properties(title=sname)
    )

    return h.interactive()


def agg_n_modes(df):
    df = df[["d", "val", "agg_val"]].copy()
    # For some tests, `agg_val` is null. If so, replace with `val`
    df["agg_val"] = df[["val", "agg_val"]].fillna(
        method="ffill", axis=1
    )["agg_val"]
    res = (
        df.groupby("d")
        .agg({"val": n_modes, "agg_val": geo_mean})
        .reset_index(drop=0)
        .rename(columns={"val": "n_modes", "d": "date"})
    )
    return res


def _build_facets(plat_df, A, n_cols=2, suites=None):
    assert (
        suites is not None
    ), "Pass `suites` as kw arg in platforms2json"
    rows = []
    for srow in z.partition(n_cols, suites, pad=None):
        row = []
        for sname in (s for s in srow if s):
            gdf = plat_df.query("sname == @sname")
            if not len(gdf):
                continue
            pdf = agg_n_modes(gdf)
            row.append(pl_suite_modes(pdf, sname, A))
        if len(row):
            row_plot = A.hconcat(*row)
            rows.append(row_plot)

    return A.vconcat(*rows)
