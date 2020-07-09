/*
- deduped test version

Filters Based on  Performance: Release Criteria Master
https://sql.telemetry.mozilla.org/queries/64399/source

Common projects: 'mozilla-central', 'try', 'autoland',
  'mozilla-esr68', 'mozilla-release', 'comm-central', 'try-comm-central',
  'fenix', 'mozilla-beta', 'comm-beta'

Common frameworks: 'job_resource_usage', 'vcs', 'build_metrics',
  'raptor', 'platform_microbench', 'talos', 'awsy',
  'browsertime', 'devtools', 'js-bench'
*/

CREATE TEMP FUNCTION bh_start_date() returns date AS (
  '{BH_START_DATE}'
);

CREATE TEMP FUNCTION major_vers(st string) AS (
  -- '10.0' => 10
  cast(regexp_extract(st, '(\\d+)\\.?') as int64)
);

-- Talos tests that measure release criteria
CREATE TEMP FUNCTION isDesiredTalos(name STRING) AS (
    name IN ('tabpaint', 'tabswitch', 'startup_about_home_paint', 'sessionrestore')
);

  
-- Limit the query to the time, projects, frameworks, platforms, and tests of interest
CREATE TEMP FUNCTION isReleaseCriteria_notime(framework STRING, project STRING, platform STRING, name STRING) AS (
    project IN ('fenix', 'mozilla-beta')  -- Projects of interest 'mozilla-central'
    AND framework IN ('raptor', 'talos', 'awsy', 'browsertime') -- Frameworks of interest
    AND platform IN (
      'android-hw-g5-7-0-arm7-api-16-shippable',
      'android-hw-p2-8-0-android-aarch64-shippable',
      'linux64-shippable',
      'macosx1014-64-shippable',
      'windows10-64-shippable',
      'windows7-32-shippable'
    )
    AND ( -- Release criteria related tests as of FF72 and Fenix GA
        isDesiredTalos(name)
        OR STARTS_WITH(name, 'raptor-speedometer')
    )
);

-- Simplify the test name to improve readability and aid aggregation
CREATE TEMP FUNCTION simplifyTestName(name STRING) AS (
    CASE --Simplify names of raptor tests (i.e. remove adjectives)
        WHEN STARTS_WITH(name, "raptor-tp6") AND ENDS_WITH(name, '-cold') THEN 'tp6-cold'
        WHEN STARTS_WITH(name, "raptor-tp6") THEN 'tp6-warm'
        WHEN STARTS_WITH(name, "raptor-speedometer") THEN 'speedometer'
        WHEN REGEXP_CONTAINS(name, r"raptor-scn-power-idle-(fen|geckoview)") THEN 'power-idle-foreground'
        ELSE name
    END
);

-- Parse the operating system name from the platform string
CREATE TEMP FUNCTION parseOS (platform STRING) AS (
    CASE
        WHEN STARTS_WITH(platform, "linux") THEN "linux"
        WHEN STARTS_WITH(platform, "macosx") THEN "osx"
        WHEN STARTS_WITH(platform, "windows7") THEN "windows7"
        WHEN STARTS_WITH(platform, "windows10") THEN "windows10"
        WHEN STARTS_WITH(platform, "android") THEN "android"
        ELSE platform
    END
);

-- Set firefox as the browser for Talos tests and parse the browsername from Raptor test names where possible.
CREATE TEMP FUNCTION identifyBrowserTested(name STRING) AS (
    CASE
        WHEN isDesiredTalos(name) THEN 'firefox'
        ELSE REGEXP_EXTRACT(name, "(firefox|fenix|fennec|fennec64|fennec68|chromium|geckoview|chrome|refbrow)")
    END
);

CREATE TEMP FUNCTION ndots(s string) AS (
  -- Number of '.' in s
  length(s) - length(replace(s, '.', ''))
);

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
    and date(build.build.date) >= bh_start_date() -- START_DATE
    -- don't want dot-versions, like 72.0.2
    and ndots(build.target.version) < 2
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
  coalesce(next_release_date, '{END_DATE}') as next_release_date
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
            unnest( GENERATE_DATE_ARRAY(bh_start_date(), '{END_DATE}', interval 1 day) ) as day 
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


, base as (
SELECT
  perf.platform,
  perf.time,
  perf.project,
  parseOS(perf.platform) as os,
  coalesce(s.value, st.value) as y,

  coalesce(def.label, '<n>') as label,
  -- This is for dropping duplicate rows for speedometer results
  -- with the same aggregated test value. They should be unique
  -- by taskId + suite name (throwing label in for good measure, though)
  -- Label example: test-windows10-64-shippable/opt-talos-other-e10s
  row_number() over (partition by
      perf.taskId, s.name, perf.platform, def.label,
      ARRAY_TO_STRING(s.extraOptions, " ")) as test_row,
  
  s.name as suite,
  identifyBrowserTested(s.name) as browser,
  simplifyTestName(s.name) as sname,
  st.shouldAlert,
  st.name as st_name,
  ARRAY_TO_STRING(s.extraOptions, " ") as extra_options,

  EXISTS (
  SELECT
    s.name,
    s.extraOptions
  FROM
    UNNEST(perf.suites) AS s
  WHERE
    s.name = alert.suite
    AND alert.extra_options = ARRAY_TO_STRING(s.extraOptions, " ")) as alerted

  -- perf.taskId,
  -- perf.taskGroupId,
  -- array_length(st.replicates) as n_reps,
  -- perf.framework,
  -- s.value as agg_val,
  -- st.value as val,  -- loadtime
  -- substr(perf.revision, 0, 6) as rev,
  -- revision,
  -- perf.symbol,
  -- perf.groupSymbol,
  -- perf.recordingDate as recording_date,
FROM
  taskclusteretl.perfherder AS perf
LEFT JOIN
  taskclusteretl.perfherder_alert AS alert
ON
  perf.revision = alert.push_revision
  AND perf.framework = alert.framework
  AND perf.platform = alert.platform
  , UNNEST(perf.suites) AS s,
  UNNEST(S.subtests) as st
left join (
  select
    distinct taskId,
    ARRAY(SELECT mv.value from unnest(tags) as mv where mv.key = 'label')[SAFE_OFFSET(0)] as label,
  from `taskclusteretl.task_definition`
  ) as def on def.taskId = perf.taskId
WHERE
  date(perf.time) > "2019-07-01" -- min date of db
  and date(perf.time) between '{START_DATE}' and '{END_DATE}'
  and isReleaseCriteria_notime(perf.framework, project, perf.platform, s.name)
)


select
  b.*,
  vers.version as bvers,
  major_vers(vers.version) as mvers
from base b
left join day_vers vers
  on date(b.time) = vers.day
where if(sname in ('speedometer', 'tabswitch'), test_row = 1, true)