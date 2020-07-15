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
    summary = f"""start_date: {start_date}
    end_date: {end_date}
    bh_start_date: {bh_start_date}
    """
    print(summary)
    return

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
        start_date=start_date,
        till_yesterday=True,
        just_dates=just_dates,
    )
    return q


def fmt_test_data_query_yesterday(just_dates=False):
    yesterday = to_subdate(dt.date.today() - dt.timedelta(days=1))
    q = fmt_test_data_query(
        end_date=yesterday, start_date=yesterday, just_dates=just_dates
    )
    return q


def extract_upload_test_data(
    bq_query, start_date=None, ignore_existing=True
):
    sql = fmt_test_data_query(
        till_yesterday=True,
        fill_yesterday=False,
        backfill=False,
        end_date=None,
        bh_start_date=None,
        start_date=start_date,
    )
    df = bq_query(sql)
    # return df
    if ignore_existing:
        existing_dates = bq.pull_existing_dates(
            bq.bq_locs["test"], date_field="time", convert_to_date=True
        )
        upload_bm = ~df.time.dt.date.isin(existing_dates.dt.date)
        print(
            "Duplicate rows found. Only uploading "
            f"{upload_bm.sum()} / {len(df)}"
        )
        df = df[upload_bm]

    client = bq.get_client()
    client.load_table_from_dataframe(df, bq.tables["test"])


if __name__ == "__main__":
    Fire(fmt_test_data_query)
