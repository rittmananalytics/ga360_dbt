WITH
  allevents AS (
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
    hits.eventInfo.eventCategory, hits.eventInfo.eventAction, hits.eventInfo.eventLabel, hits.eventInfo.eventValue
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` t,
    UNNEST(t.hits) AS hits
  WHERE
    hits.type = 'EVENT')
SELECT
  *,
  LAG(hitTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY hitTimestamp) previousHitTimestamp,
  TIMESTAMP_DIFF(hitTimestamp,LAG(hitTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY hitTimestamp),SECOND) secondsSinceLastVisitorHit
FROM
  allevents
