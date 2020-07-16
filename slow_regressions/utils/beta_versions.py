import datetime as dt
import re
import toolz.curried as z  # type: ignore
from typing import Union

import numpy as np  # type: ignore
import pandas as pd  # type: ignore

# import perf.utils.perf_utes as pfu
import slow_regressions.utils.slow_reg_utils as sru

lmap = z.compose(list, map)


def ts2str(ts: pd.Timestamp):
    return str(ts.date())


def get_bnum(s: str, pat=re.compile(r"\d+.\db(\d+)")) -> int:
    "'70.0b3' -> 3; '70.0' -> -1"
    res = pat.findall(s)
    if not res:
        return -1
    [i] = res
    return int(i)


def test_get_bnum():
    assert lmap(
        get_bnum, ["77.0b0", "", "77.0", "101.0b13", "xbx"]
    ) == [0, -1, -1, 13, -1]


###########################################
# Munging to find relevant tests/versions #
# for comparison                          #
###########################################
"""
Need to easily
* pick dates (`get_prev_vers_dates(current_date, beta_vers)`)
    * previous Wed
    * Wed before that
    * previous major versions
* all of the n tests leading up to that date
    * this can be done with
```
df[["time", "y"]].query("time <= @day").tail(6)
```
for a day from `get_prev_vers_dates`
"""


@sru.requires_cols_kw(
    beta_vers=["day", "mvers", "betav", "dot_release", "dw"]
)
def pick_latest_mvers(mvers: int, *_, beta_vers=None):
    """
    beta_vers.dw: day of week, 1st 3 letters. ('Wed', ...)
    Wed seems to be the convention for day of the week
    these values are reported.
    """

    this_mvers = (
        # betav == -1 when it doesn't have a `b`
        # betav('73.0b3') == 3; betav('73.0') == -1
        beta_vers.query("betav == -1")
        .query("~dot_release")
        .query("mvers == @mvers")
    )
    dows = this_mvers.dw.pipe(set)
    if "Wed" in dows:
        this_mvers = this_mvers.query("dw == 'Wed'")
    elif "Thu" in dows:
        this_mvers = this_mvers.query("dw == 'Thu'")
    if not len(this_mvers):
        return None
    this_mvers_std = this_mvers.sort_values(["day"], ascending=True)
    return this_mvers_std.day.iloc[-1]


@sru.requires_cols_kw(
    beta_vers=["day", "mvers", "betav", "dot_release", "dw"]
)
def get_prev_vers_dates(
    current_date: Union[str, dt.datetime], beta_vers: pd.DataFrame
) -> "pd.DataFrame":
    current_date = pd.to_datetime(current_date)

    beta_vers_day = beta_vers.set_index("day")
    _1w = pd.Timedelta("1w")
    prev_weeks = [current_date - _1w, current_date - _1w - _1w]
    cur_mvers = beta_vers_day.mvers.loc[current_date]
    prev_mvers_date = pick_latest_mvers(
        cur_mvers - 1, beta_vers=beta_vers
    )
    prev_mvers_date2 = pick_latest_mvers(
        cur_mvers - 2, beta_vers=beta_vers
    )
    dates = (
        pd.DataFrame(
            dict(
                prev_dates=pd.to_datetime(
                    prev_weeks + [prev_mvers_date, prev_mvers_date2]
                ),
                prev_vers=[
                    "prev_week",
                    "prev_week2",
                    f"v{cur_mvers - 1}",
                    f"v{cur_mvers - 2}",
                ],
                date_type=[
                    "prev",
                    "prev",
                    "major_version",
                    "major_version",
                ],
                date_label=list(pd.to_datetime(prev_weeks).map(ts2str))
                + [f"v{cur_mvers - 1}", f"v{cur_mvers - 2}"],
            )
        ).set_index("prev_vers")
        # .prev_dates
    )
    return dates
