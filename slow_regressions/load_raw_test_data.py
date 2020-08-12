import datetime as dt
from typing import Union

from slow_regressions import app_dir
from slow_regressions.utils import bq_utils as bq

from fire import Fire


def to_subdate(d):
    return d.strftime("%Y-%m-%d")


def from_subdate(s):
    return dt.datetime.strptime(s, "%Y-%m-%d")


def subdate_diff(subdate, **diff_kw):
    date = from_subdate(subdate)
    new_date = date - dt.timedelta(**diff_kw)
    return to_subdate(new_date)


def fmt_test_data_query(
    till_yesterday=True,
    end_date=None,
    bh_start_date=None,
    start_date: Union[str, int] = 0,
    just_dates=False,
    stdout=True,
):
    """
    @start_date: if int, set to n days before `end_date`.
    @bh_start_date: if None, set to 30 days before
      `start_date`.
    """
    if till_yesterday:
        end_date = to_subdate(dt.date.today() - dt.timedelta(days=1))
    if isinstance(start_date, int):
        start_date = subdate_diff(end_date, days=start_date)

    bh_start_date = bh_start_date or subdate_diff(start_date, days=30)

    if just_dates:
        return dict(
            start_date=start_date,
            end_date=end_date,
            bh_start_date=bh_start_date,
        )

    floc = app_dir / "data/test_data.sql"
    with open(floc, "r") as fp:
        sql = fp.read()

    sql = sql.format(
        BH_START_DATE=bh_start_date,
        START_DATE=start_date,
        END_DATE=end_date,
    )
    if stdout:
        print(sql)
    else:
        return sql


def fmt_test_data_query_backfill(just_dates=False):
    start_date = "2019-07-01"
    q = fmt_test_data_query(
        till_yesterday=True,
        start_date=start_date,
        just_dates=just_dates,
    )
    return q


def fmt_test_data_query_yesterday(just_dates=False):
    q = fmt_test_data_query(
        till_yesterday=True, start_date=0, just_dates=just_dates
    )
    return q


def extract_upload_test_data(
    bq_query=None,
    end_date=None,
    start_date=0,
    skip_existing=True,
    bq_loc=bq.bq_locs.test,
):
    """
    Download summary data from
    `moz-fx-data-derived-datasets.taskclusteretl.perfherder` to
    temp table.
    """
    sql = fmt_test_data_query(
        till_yesterday=True,
        start_date=start_date,
        stdout=False,
        end_date=end_date,
    )
    bq_query = bq_query or bq.bq_query2
    df = bq_query(sql)
    # return df
    if skip_existing:
        df = bq.filter_existing_dates(
            df,
            bq_loc=bq_loc,
            convert_to_date=True,
            date_field="time",
        )
        if df is None:
            return

    client = bq.get_client()
    client.load_table_from_dataframe(df, bq_loc.no_tick)


def get_yesterday_subdate():
    return to_subdate(dt.date.today() - dt.timedelta(days=1))


if __name__ == "__main__":
    Fire(
        {
            "sql": fmt_test_data_query,
            "etl": extract_upload_test_data,
            "yesterday": get_yesterday_subdate,
        }
    )
