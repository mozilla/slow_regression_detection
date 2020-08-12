with test_data as (
select
  distinct date(time) as date, 'test_data' as table
from moz-fx-data-shared-prod.analysis.wbeard_test_slow_regression_test_data
order by date desc
limit 7
)
, input_data as (
select
  distinct date, 'input_data' as table
from moz-fx-data-shared-prod.analysis.wbeard_slow_regression_input_data_test
order by date desc
limit 7
)
, samples as (
select
  distinct date, 'samples' as table
from moz-fx-data-shared-prod.analysis.wbeard_slow_regression_draws_test
order by date desc
limit 7
)
select * from test_data
union all
select * from input_data
union all
select * from samples