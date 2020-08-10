import datetime as dt

from fire import Fire
import pandas as pd  # type: ignore
import numpy as np  # type: ignore

import slow_regressions.utils.bq_utils as bq


def make_date_query(sql_loc, dt_field=None):
    if dt_field:
        date_def = f"date({dt_field}) as"
    else:
        date_def = ""

    q = f"""
    select
      distinct {date_def} date
    from {sql_loc}
    order by date
    """
    return q


def check_table_dates(bq_locs=bq.bq_locs):
    # global test_dates, samp_dates, inp_dates
    q_test = make_date_query(bq_locs.test.sql, dt_field="time")
    q_samps = make_date_query(bq_locs.samples.sql, dt_field="")
    q_inp = make_date_query(bq_locs.input_data.sql, dt_field="")

    test_dates = bq.bq_query2(q_test)
    samp_dates = bq.bq_query2(q_samps)
    inp_dates = bq.bq_query2(q_inp)

    print(f"\n\ntest table: {bq_locs.test.sql}")
    print(bq.find_missing(test_dates.date, lastn=20))

    print(f"\n\nsamp_dates table: {bq_locs.samples.sql}")
    print(bq.find_missing(samp_dates.date, lastn=20))

    print(f"\n\nq_inp table: {bq_locs.input_data.sql}")
    print(bq.find_missing(inp_dates.date, lastn=20))


if __name__ == "__main__":
    Fire({"dates": check_table_dates})
