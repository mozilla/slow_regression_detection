import datetime as dt
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


class BqLocation:
    def __init__(
        self,
        table,
        dataset="analysis",
        project_id="moz-fx-data-shared-prod",
        base_project_id="moz-fx-data-shared-prod",
    ):
        self.table = table
        self.dataset = dataset
        self.project_id = project_id
        self.base_project_id = base_project_id

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
