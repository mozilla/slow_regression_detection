select
  *,
from `moz-fx-data-shared-prod.analysis.wbeard_slow_regression_input_data_test`
where
  suite = 'startup_about_home_paint'  
  and platform = 'linux64-shippable'
limit 5