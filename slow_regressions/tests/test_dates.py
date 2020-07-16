import datetime as dt
from pathlib import Path
import sys

test_dir = Path(__file__).parent
proj_dir = test_dir.parent.parent
sys.path.insert(0, str(proj_dir))

import slow_regressions.data.gen_test_data_query as gtd  # noqa

"""
cd /sreg
py.test slow_regressions/tests/test_dates.py -s
"""


class NewDate(dt.date):
    "https://stackoverflow.com/a/4482067/386279"

    @classmethod
    def today(cls):
        return cls(2020, 7, 10)


dt.date = NewDate


def test_backfill_dates():
    res = gtd.fmt_test_data_query_backfill(just_dates=True)
    shoulbe = {
        "start_date": "2019-07-01",
        "end_date": "2020-07-09",
        "bh_start_date": "2019-06-01",
    }
    assert res == shoulbe


def test_yesterday_dates():
    res = gtd.fmt_test_data_query_yesterday(just_dates=True)
    shoulbe = {
        "start_date": "2020-07-09",
        "end_date": "2020-07-09",
        "bh_start_date": "2020-06-09",
    }
    assert res == shoulbe


def test_dates():
    res = gtd.fmt_test_data_query(
        till_yesterday=False,
        end_date="2020-06-11",
        start_date=1,
        just_dates=True,
    )
    shoulbe = {
        "start_date": "2020-06-10",
        "end_date": "2020-06-11",
        "bh_start_date": "2020-05-11",
    }
    assert res == shoulbe
