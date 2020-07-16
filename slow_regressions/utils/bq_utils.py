import datetime as dt
from functools import lru_cache
import subprocess
import tempfile

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import pandas_gbq as pbq  # type: ignore

from google.cloud.bigquery import Client
import slow_regressions.utils.slow_reg_utils as sru

# analysis = "moz-fx-data-shared-prod.analysis.{}".format


tables = sru.AttrDict(
    test="moz-fx-data-shared-prod.analysis.wbeard_test_slow_regression_test_data",  # noqa
    input_data="moz-fx-data-shared-prod.analysis.wbeard_slow_regression_input_data_test",  # noqa
    samples="moz-fx-data-shared-prod.analysis.wbeard_slow_regression_draws_test",  # noqa
)


def bq_query(sql):
    "Hopefully this just works"
    return pbq.read_gbq(sql)


@lru_cache()
def get_client():
    return Client(project="moz-fx-data-bq-data-science")


def bq_query2(sql):
    client = get_client()
    return client.query(sql).to_dataframe()


def bq_upload(df, table):
    client = get_client()
    client.load_table_from_dataframe(df, table)


@lru_cache()
def to_subdate(d):
    return d.strftime("%Y-%m-%d")


@lru_cache()
def from_subdate(s):
    return dt.datetime.strptime(s, "%Y-%m-%d")


def subdate_diff(subdate, **diff_kw):
    date = from_subdate(subdate)
    new_date = date - dt.timedelta(**diff_kw)
    return to_subdate(new_date)


def is_subdate(s):
    try:
        from_subdate(s)
    except Exception:
        return False
    return True


class BqLocation:
    def __init__(
        self,
        table,
        dataset="analysis",
        project_id="moz-fx-data-shared-prod",
        cred_project_id="moz-fx-data-bq-data-science",
    ):
        self.table = table
        self.dataset = dataset
        self.project_id = project_id
        self.cred_project_id = cred_project_id

    @property
    def sql(self):
        return f"`{self.project_id}`.{self.dataset}.{self.table}"

    @property
    def no_tick(self):
        return f"{self.project_id}.{self.dataset}.{self.table}"

    @property
    def cli(self):
        return f"{self.project_id}:{self.dataset}.{self.table}"

    @property
    def no_proj(self):
        return f"{self.dataset}.{self.table}"

    @property
    def sql_dataset(self):
        return f"`{self.project_id}`.{self.dataset}"

    @staticmethod
    def from_sql(sql_str):
        parts = sql_str.split(".")
        if len(parts) != 3:
            raise ValueError("`from_sql` takes 3 names separated by .")
        project, dataset, table = parts
        return BqLocation(table, dataset=dataset, project_id=project)


bq_locs = sru.AttrDict(
    {k: BqLocation.from_sql(table) for k, table in tables.items()}
)


def run_command(cmd, success_msg="Success!"):
    """
    @cmd: List[str]
    No idea why this isn't built into python...
    """
    try:
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT
        ).decode()
        success = True
    except subprocess.CalledProcessError as e:
        output = e.output.decode()
        success = False

    if success:
        print(success_msg)
        print(output)
    else:
        print("Command Failed")
        print(output)


def upload_cli(
    df,
    bq_dest: BqLocation,
    # project_id="moz-fx-data-derived-datasets",
    # analysis_table_name="wbeard_crash_rate_raw",
    # cred_project_id="moz-fx-data-bq-data-science",
    add_schema=False,
    dry_run=False,
    replace=False,
    time_partition=None,
):
    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as fp:
        df.to_csv(fp, index=False, na_rep="NA")
    print("CSV saved to {}".format(fp.name))
    # return fp.name
    cmd = [
        "bq",
        "load",
        "--replace" if replace else "--noreplace",
        "--project_id",
        bq_dest.cred_project_id,
        "--source_format",
        "CSV",
        "--skip_leading_rows",
        "1",
        "--null_marker",
        "NA",
    ]
    if time_partition is not None:
        cmd.extend(["--time_partitioning_field", time_partition])

    cmd += [bq_dest.cli, fp.name]
    if add_schema:
        schema = get_schema(df, as_str=True)
        print(schema)
        cmd.append(schema)

    print(" ".join(cmd))
    success_msg = f"Success! Data uploaded to {bq_dest.cli}"
    if not dry_run:
        run_command(cmd, success_msg)


def get_schema(df, as_str=False, **override):
    dtype_srs = df.dtypes
    dtype_srs.loc[
        dtype_srs.map(lambda x: np.issubdtype(x, np.datetime64))
    ] = "TIMESTAMP"
    dtype_srs.loc[dtype_srs == "category"] = "STRING"
    dtype_srs.loc[dtype_srs == "float64"] = "FLOAT64"
    dtype_srs.loc[dtype_srs == np.int] = "INT64"
    dtype_srs.loc[dtype_srs == object] = "STRING"
    dtype_srs.loc[dtype_srs == bool] = "BOOL"
    manual_dtypes = dict(
        date="DATE",
        # c_version_rel="DATE", major="INT64", minor="INT64"
    )
    dtype_srs.update(pd.Series(manual_dtypes))
    print(dtype_srs)
    dtype_srs.update(pd.Series(override))
    missing_override_keys = set(override) - set(dtype_srs.index)
    if missing_override_keys:
        raise ValueError(
            "Series missing keys {}".format(missing_override_keys)
        )

    non_strings = dtype_srs.map(type).pipe(lambda x: x[x != str])
    if len(non_strings):
        raise ValueError(
            "Schema values should be strings: {}".format(non_strings)
        )
    if not as_str:
        return dtype_srs
    res = ",".join(
        ["{}:{}".format(c, t) for c, t in dtype_srs.items()]
    )
    return res


def pull_existing_dates(
    bq_loc, date_field="date", convert_to_date=False
):
    if convert_to_date:
        date_field = f"date({date_field})"
    q = f"""
    select distinct {date_field}
    from {bq_loc.sql}
    order by 1
    """
    return bq_query(q).iloc[:, 0]


def filter_existing_dates(
    df, bq_loc, convert_to_date=False, date_field="date"
):
    dates_to_upload = df[date_field]
    if not is_subdate(dates_to_upload.iloc[0]):
        print("Converting dates to strings")
        dates_to_upload = dates_to_upload.map(to_subdate)

    existing_dates = (
        pull_existing_dates(
            bq_loc,
            convert_to_date=convert_to_date,
            date_field=date_field,
        )
        .map(to_subdate)
        .pipe(set)
    )
    bm = ~dates_to_upload.isin(existing_dates)
    print(f"About to upload {bm.sum()} / {len(bm)} rows")
    df = df[bm]
    if not len(df):
        print("Nothing new to upload")
        return None
    return df
