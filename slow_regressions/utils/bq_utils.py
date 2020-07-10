import datetime as dt
import subprocess
import tempfile

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import pandas_gbq as pbq  # type: ignore


def bq_query(sql):
    "Hopefully this just works"
    return pbq.read_gbq(sql)


def to_subdate(d):
    return d.strftime("%Y-%m-%d")


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
    def cli(self):
        return f"{self.project_id}:{self.dataset}.{self.table}"

    @property
    def no_proj(self):
        return f"{self.dataset}.{self.table}"

    @property
    def sql_dataset(self):
        return f"`{self.project_id}`.{self.dataset}"


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
