DECLARE empty___, END_DATE date;
SET (empty___, END_DATE) = ('2020-02-10', '2020-05-06');
CREATE TEMP FUNCTION StripFirstLast(arr ANY type) AS (
  -- https://stackoverflow.com/a/48329058/386279
  ARRAY(SELECT x FROM UNNEST(arr) AS x WITH OFFSET
  WHERE OFFSET BETWEEN 1 AND ARRAY_LENGTH(arr) - 2)
);
with draws_all as (
select *
      from `moz-fx-data-shared-prod.analysis.wbeard_slow_regression_draws_test`
      where date in (
        END_DATE,
        date_sub(END_DATE, INTERVAL 7 day),
        date_sub(END_DATE, INTERVAL 14 day)
        )
)
, draws as (
select
  a.*,
  (c.y - a.y) / a.y as y_diff
from (select * from draws_all where date <> END_DATE) a -- all
inner join (select * from draws_all where date = END_DATE) c -- current
  on a.suite = c.suite
  and a.platform = c.platform
  and a.index = c.index
)
, draws_pct as (
select
  suite,
  platform,
  date,
--   approx_quantiles(y_diff, 100) as aq,
  approx_quantiles(y_diff, 10) as aq2,
--   approx_perct(y_diff, 10) as yd10,
--   StripFirstLast(approx_quantiles(y_diff, 2)) as mid
from draws
group by 1, 2, 3
)
select
  suite,
  platform,
  date,
--   aq[offset(10)] as p10,
--   aq[offset(50)] as p50,
--   aq[offset(90)] as p90,
  aq2[offset(1)] as p10,
  aq2[offset(5)] as p50,
  aq2[offset(9)] as p90,
from draws_pct dr
where
  dr.suite = 'tabswitch' -- 'startup_about_home_paint'
-- order by date desc  
-- limit 5