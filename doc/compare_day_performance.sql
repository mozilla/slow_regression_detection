DECLARE START_DATE_, END_DATE date;
SET (START_DATE_, END_DATE) = ('2020-02-10', '2020-05-06');
CREATE TEMP FUNCTION StripFirstLast(arr ANY type) AS (
  -- https://stackoverflow.com/a/48329058/386279
  ARRAY(SELECT x FROM UNNEST(arr) AS x WITH OFFSET
  WHERE OFFSET BETWEEN 1 AND ARRAY_LENGTH(arr) - 2)
);
-- CREATE TEMP FUNCTION approx_perct(arr any type, n int64) AS (
--   (select approx_quantiles(reps, 2)[offset(1)] from unnest(arr) as reps)
-- );
-- CREATE TEMP FUNCTION approx_perct(arr any type, n int64) AS (
--   APPROX_QUANTILES(arr, 100)[OFFSET(n)]
-- );
with draws_all as (
select *
      from `moz-fx-data-shared-prod.analysis.wbeard_slow_regression_draws_test`
      where date in (
        END_DATE,
        date_sub(END_DATE, INTERVAL 7 day))
)
, draws as (
select
  prev.*,
  (this.y - prev.y) / prev.y as y_diff
from (select * from draws_all where date <> END_DATE) prev -- all
inner join (select * from draws_all where date = END_DATE) this -- current
  on prev.suite = this.suite
  and prev.platform = this.platform
  and prev.index = this.index
)
, draws_pct as (
select
  suite,
  platform,
  date,
  approx_quantiles(y_diff, 100) as aq,
--   approx_perct(y_diff, 10) as yd10,
--   StripFirstLast(approx_quantiles(y_diff, 2)) as mid
from draws
group by 1, 2, 3
)
select
  suite,
  platform,
  date,
  aq[offset(10)] as p10,
  aq[offset(50)] as p50,
  aq[offset(90)] as p90,
from draws_pct dr
where
  dr.suite = 'tabswitch' -- 'startup_about_home_paint'
-- order by date desc  
-- limit 5