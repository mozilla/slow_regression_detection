
from slow_regressions.utils.bq_utils import (
    BqLocation,
    bq_query,
    subdate_diff,
)


def load(
    date,
    bq_loc=BqLocation(
        "wbeard_test_slow_regression_test_data",
        dataset="analysis",
        project_id="moz-fx-data-shared-prod",
    ),
    history_days=365,
):
    date_start = subdate_diff(date, days=history_days)
    q = f"""
    CREATE TEMP FUNCTION major_vers(st string) AS (
      -- '10.0' => 10
      cast(regexp_extract(st, '(\\\\d+)\\\\.?') as int64)
    );

    select
      *,
      major_vers(bvers) as mvers
    from {bq_loc.sql}
    where
      date(time) between '{date_start}' and '{date}'
    """
    return bq_query(q)


def transform_test_data(df):
    df = df.assign(d=lambda x: x.time.dt.date)
    return df


def write_weds(this_wed):
    for (sname, platform), gdf in dfbaadd.query(
        "d <= @this_wed"
    ).groupby([c.sname, c.platform]):
        outdir = Path(
            f"../data/cage/{this_wed.replace('-', '_')}/suite_data"
        )
        outdir.mkdir(parents=True, exist_ok=True)
        rw.ts_write(sname, platform, df=gdf, outdir=outdir)
    return outdir
