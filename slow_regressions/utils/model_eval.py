from glob import glob
from pathlib import Path
import re

import attr
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import scipy.stats as sts  # type: ignore

from slow_regressions.utils import beta_versions as bv
from slow_regressions.stats import modal

import slow_regressions.utils.slow_reg_utils as sru


def ts2str(ts: pd.Timestamp):
    return str(ts.date())


def k2plat(k):
    return k.split("__")[1]


def k2suite(k):
    return k.split("__")[0]


def k2os(k):
    plat = k2plat(k).replace("_", "-")
    os = sru.platform2os(plat)
    return os


def series_in_bounds(df, lbcol="p25", ubcol="p75", keep_daily_rows=2):
    """
    This gives error bounds around whether 50% of the
    points are within the 50% credible intervals.
    Randomly pick `keep_daily_rows` per day.
    """
    df = df[["d", "y", lbcol, ubcol]]
    df = df.copy().assign(
        rand_row=lambda x: x.groupby("d").y.transform(
            modal.choose_rand, n=keep_daily_rows
        )
    )
    within_intervals = (
        df.query("rand_row")
        .eval(f"y < {ubcol} & y > {lbcol}")
        .value_counts(normalize=0)
        .to_frame()
        .T.rename(columns={True: "a", False: "b"})
    )
    return sts.beta(within_intervals.a, within_intervals.b).ppf(
        [0.05, 0.95]
    )


def build_reader(pat, pref="ts-"):
    def proc(s):
        s = Path(s).stem
        s = s[len(pref) :]  # noqa
        return re.sub(r"[^A-z\d_]+", "_", s)

    fns = glob(pat)

    obj = sru.AttrDict({proc(f): f for f in fns})
    return obj


def get_str_key(fn, pat=re.compile(r"ts-(.+)")):
    fn = Path(fn).stem
    [res] = pat.findall(fn)
    if not res:
        print(fn)
    return res


@sru.requires_cols(["p10", "p90"])
def classify_regr(df, suite):
    lib = sru.lower_is_better[suite]
    if lib:
        return df["p10"] > 0
        # return df["p10"] < 0
    return df["p90"] < 0


@attr.s
class SuitePlat:
    k = attr.ib()
    data_inp = attr.ib()
    daterange_map = attr.ib()
    pi = attr.ib()
    draws = attr.ib()
    df = attr.ib()
    # return k, data_inp, daterange_map, pi, draws, df

    @property
    def suite(self):
        return self.k.split("__")[0]

    @property
    def platform(self):
        return self.k.split("__")[1]


class ModelManager:
    """

    """
    def __init__(self, base_dir, date_str, beta_vers):
        self.base = Path(base_dir)
        # self.wed = wed
        # self.d = self.base / wed
        self.fth_dirs = {"suite_data", "br_draws", "brpi"}

        self.date_str = date_str
        self.date = pd.to_datetime(self.date_str)
        self.ks = [get_str_key(fn) for fn in self.br_draws.glob("*")]

        def fmt_k(k):
            data_inp = self.load("suite_data", k)
            daterange_map = self.get_dayi2date(df=data_inp)
            pi = self.load("brpi", k).pipe(process_pi)
            draws = self.load("br_draws", k).pipe(
                self.format_draws, daterange_map=daterange_map
            )
            # import ipdb; ipdb.set_trace()
            df = combine_data_pi(data_inp, pi)
            day2mvers = beta_vers.set_index("day").mvers.to_dict()
            df["mvers"] = df.d.map(day2mvers)
            data_inp["mvers"] = data_inp.d.map(day2mvers)
            data_inp["n_modes"] = data_inp.groupby(
                ["mvers"]
            ).y.transform(modal.n_modes).fillna(-1).astype(int)
            sp = SuitePlat(
                k=k,
                data_inp=data_inp,
                daterange_map=daterange_map,
                pi=pi,
                draws=draws,
                df=df,
            )
            return sp

        self.sps = {k: fmt_k(k) for k in self.ks}
        self.suite_plats = list(self.sps.values())

        self.prev_days = bv.get_prev_vers_dates(
            self.date_str, beta_vers=beta_vers
        )

    def calc_diffs(
        self,
        suite_plat,
        pct=True,
        agg=False,
        negate_agg=False,
        calc_regressions=False,
    ):
        draws_last_day = suite_plat.draws.iloc[:, -1]
        prev_days_str = self.prev_days.prev_dates.map(ts2str)
        try:
            diff_draws = -suite_plat.draws[prev_days_str].sub(
                draws_last_day, axis=0
            )
        except KeyError:
            missing_days = set(prev_days_str) - set(suite_plat.draws)
            print(f"Warning: {suite_plat.k} is missing {missing_days}")
            return None

        if pct:
            diff_draws = diff_draws.div(draws_last_day, axis=0) * 100

        # [Timestamp('2020-04-29 00:00:00'), ...] * 4
        # => 2020-04-29 2020-04-22 v76 v75

        def classify_date_cols(diff_draws):
            """
            prev_days looks like
                          prev_vers
            prev_week    2020-04-29
            prev_week2   2020-04-22
            v76          2020-04-29
            v75          2020-04-01
            Where first 2 days are chosen based on same days
            a number of weeks ago, and the 2nd 2 are dates representing
            last estimates for previous major versions. Sometimes
            these may overlap (when a cycle was recently completed).
            Add a `date_type` column to distinguish major version from
            'weeks ago' dates.
            """
            # cols = list(diff_draws)
            # cols[-2:] = self.prev_days.index[-2:]
            # diff_draws.columns = cols
            diff_draws.columns = list(self.prev_days.date_label)
            return diff_draws

        diff_draws = classify_date_cols(diff_draws)
        date_label2prev_date = self.prev_days.set_index(
            "date_label"
        ).prev_dates.to_dict()

        if agg:
            if negate_agg:
                diff_draws = -diff_draws
                print("Negate!")
            diff_draws = (
                diff_draws.quantile([.1, .5, .9])
                .T.rename(columns=rn_prob_col)
                .reset_index(drop=0)
                .rename(columns={"index": "date_label"})
                .assign(
                    prev_date=lambda x: x.date_label.map(
                        date_label2prev_date
                    )
                )
                .sort_values(["prev_date"], ascending=True)
            )
        if calc_regressions:
            diff_draws = diff_draws.assign(
                regression=lambda df: classify_regr(
                    diff_draws, suite_plat.suite
                ).map({True: "regression", False: "no regression"})
            )
        return diff_draws

    def load(self, dir, suite_plat):
        if dir not in self.fth_dirs:
            raise ValueError(f"{dir} not in {self.fth_dirs}")
        fn = self.base / dir / f"ts-{suite_plat}.fth"
        return pd.read_feather(fn)

    def weds(self):
        return [p.stem for p in self.base.g("*")]

    # @pfu.requires_cols_kw(['d', 'out'])
    def get_dayi2date(self, df):
        mn_mx = df.query("~out").d.agg(["min", "max"])
        drng = pd.date_range(*mn_mx)
        return dict(enumerate(drng))

    def format_draws(self, df, daterange_map):
        """
        @daterange_map: Dict[int, Timestamp]
        """

        def rename_draws(xnn):
            dayi_1_index = re.findall(r"X(\d+)", xnn)[0]
            dayi = int(dayi_1_index) - 1
            ts = daterange_map[dayi]

            return ts2str(ts)

        df = df.rename(columns=rename_draws)
        return df

    @property
    def suite_data(self):
        return self.base / "suite_data"

    @property
    def br_draws(self):
        return self.base / "br_draws"

    @property
    def brpi(self):
        return self.base / "brpi"


##################################
# Predictive interval processing #
##################################
def process_pi(pi):
    return pi.pipe(avg_med).rename(columns=rn_pi)


def avg_med(pi):
    """
    predictive_interval(model, prob = .01) leaves
    2 columns: 'X49.5.', 'X50.5.'
    - average these to leave the df with X50
    """
    pi = pi.copy()
    if ("X49.5." not in pi) or ("X50.5." not in pi):
        return pi
    p49 = pi.pop("X49.5.")
    p51 = pi.pop("X50.5.")
    pi["X50."] = np.mean([p49, p51], axis=0)
    return pi


def rn_pi(s):
    """
    'X49.' -> 'p49'
    'X5.' -> 'p05'
    """
    if s.startswith("X"):
        if s.count(".") > 1 and s.endswith("."):
            s = s[:-1]
        [num] = re.findall(r"\d+", s)
        return f"p{int(num):02}"
    return s


@sru.requires_cols(["time", "d", "y", "dayi", "z_min_abs", "out"])
def combine_data_pi(data, pi):
    """
    `pi`, aka predictive_intervals from brms, has columns
    `p05` that give 5th percentile posterior estimates of the model.
    Each row is for a day in the dataset, where the min and max
    correspond to the min and max of the original dataset. Every
    day in between should be accounted for, even if they were left
    out of the input data.
    """
    assert all(re.match(r"^p\d\d$", p) for p in pi)
    assert data.time.is_monotonic_increasing

    mn_mx = data.query("~out").d.agg(["min", "max"])
    pi = pi.assign(d=pd.date_range(*mn_mx)).assign(
        d=lambda df: df.d.dt.date
    )

    res = (
        data.merge(pi, on="d", how="outer")
        .assign(out=lambda df: df.out.fillna(False).astype(bool))
        .sort_values(["d", "time"], ascending=True)
        .reset_index(drop=1)
    )

    return res


def rn_prob_col(x):
    ".05 -> p05"
    if isinstance(x, int) and (x not in (0, 1)):
        return x
    if not isinstance(x, float):
        return x
    return f"p{int(x * 100):02}"
