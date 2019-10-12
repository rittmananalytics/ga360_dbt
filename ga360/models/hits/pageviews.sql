with pageviews as (
  SELECT
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS sessionId,
    fullVisitorId,
    visitId,
    TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64)) AS hitTimestamp,
    hits.hitNumber,
    hits.time/1000 AS hitsTime,
    hits.hour,
    hits.minute,
    hits.isSecure,
    hits.referer,
    hits.isExit,
    hits.isInteraction,
    hits.isEntrance,
    hits.page.pagePath,
    hits.page.hostname,
    hits.page.pageTitle,
    hits.page.searchKeyword,
    hits.page.searchCategory,
    hits.page.pagePathLevel1,
    hits.page.pagePathLevel2,
    hits.page.pagePathLevel3,
    hits.page.pagePathLevel4
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` t,
    UNNEST(t.hits) AS hits
  WHERE
    hits.type = 'PAGE')
SELECT
  *,
  LAG(hitTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY hitTimestamp) previousHitTimestamp,
  TIMESTAMP_DIFF(hitTimestamp,LAG(hitTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY hitTimestamp),SECOND) secondsSinceLastVisitorHit
FROM
  pageviews)
