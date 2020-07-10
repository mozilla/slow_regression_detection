DECLARE START_DATE, END_DATE date;
SET (START_DATE, END_DATE) = ('2019-06-01', CURRENT_DATE());

/*
For every day between START_DATE and END_DATE, return the most
recent beta version that was released as of that day.

Basic steps
* For each version, look at the lowest date from build id
  * This is ‘release date’
* Order by date, self join to get the next release date
* Create a date array (“GENERATE_DATE_ARRAY”), use a cross
    join to filter only the rows where the date fits between
    a release date and next release date
Drop duplicated dates, if there’s a tie, choose the most recently
released version on that date
*/

CREATE TEMP FUNCTION ndots(s string) AS (
  -- Number of '.' in s
  length(s) - length(replace(s, '.', ''))
);


with bh_base as 
(
  select
    build.build.date as time,
    date(build.build.date) as date,
    build.target.version,
  from
    `moz-fx-data-shared-prod`.telemetry.buildhub2 b 
  where
    b.build.target.channel = 'beta' 
    and b.build.target.os = 'win' 
    and b.build.target.locale = 'en-US' 
    and build.source.product = 'firefox' 
    and date(build.build.date) >= START_DATE
)
,
gbv1 as 
(
  select
    version,
    cast(SPLIT(version, '.')[safe_OFFSET(0)] as int64) as vers,
    count(*) as n,
    min(date) as min_date,
    row_number() over (order by min(date)) as nth_version
  from
    bh_base 
  group by
    1 
)


, gbv as (
select
  g1.*,
  coalesce(next_release_date, END_DATE) as next_release_date
from gbv1 g1
left join (
  select
    min_date as next_release_date,
    nth_version - 1 as prev_version
  from gbv1
) g2 on g1.nth_version = g2.prev_version
)

, day_vers_ as 
(
  select *,
    -- if 2 versions were released on the same day, pick max version
    row_number() over (partition by day 
  order by
    nth_version desc) as vers_ord,
  from (select day 
        from
            unnest( GENERATE_DATE_ARRAY(START_DATE, current_date(), interval 1 day) ) as day 
    )
    days cross join gbv 
  where
    days.day >= gbv.min_date 
    and days.day <= gbv.next_release_date 
)
,
day_vers as 
(
  select day, version, vers 
  from day_vers_ 
  where vers_ord = 1 
)

select
  day, version as bvers, vers as mvers, ndots(version) = 2 as dot_release
from
  day_vers
order by day


