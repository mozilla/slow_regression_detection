import datetime as dt

import click


def to_subdate(d):
    return d.strftime("%Y-%m-%d")


def from_subdate(s):
    return dt.datetime.strptime(s, "%Y-%m-%d")


def subdate_diff(subdate, **diff_kw):
    date = from_subdate(subdate)
    new_date = date - dt.timedelta(**diff_kw)
    return to_subdate(new_date)


@click.command()
@click.option("--fill_yesterday", default=False)
@click.option("--backfill", default=False)
@click.option("--start_date", default=None)
@click.option("--end_date", default=None)
def fmt_test_data_query(
    fill_yesterday=True,
    backfill=False,
    end_date=None,
    bh_start_date=None,
    start_date=None,
):
    if not backfill or fill_yesterday or start_date:
        raise ValueError(
            "At least specify `backfill` or `fill_yesterday`"
        )
    yesterday = to_subdate(dt.date.today() - dt.timedelta(days=1))

    if backfill:
        start_date = "2019-07-01"
        end_date = yesterday
    elif fill_yesterday:
        start_date = yesterday
        end_date = yesterday
    bh_start_date = subdate_diff(start_date, days=30)

    with open("test_data.sql", "r") as fp:
        sql = fp.read()

    sql = sql.format(
        BH_START_DATE=bh_start_date,
        START_DATE=start_date,
        END_DATE=end_date,
    )
    click.echo((sql))


if __name__ == "__main__":
    fmt_test_data_query()
