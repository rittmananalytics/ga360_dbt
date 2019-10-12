WITH
  sessions AS (
  SELECT
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS sessionId,
    fullVisitorId,
    CASE
      WHEN visitNumber = 1 THEN 'New'
    ELSE
    'Returning'
  END
    AS newOrReturningVisitor,
    visitId,
    visitNumber AS userSessionSeq,
    TIMESTAMP_SECONDS(visitStartTime) sessionTimestamp,
    trafficSource.referralPath,
    trafficSource.campaign,
    trafficSource.source,
    trafficSource.medium,
    trafficSource.keyword,
    trafficSource.adContent,
    device.browser,
    device.browserVersion,
    device.operatingSystem,
    device.operatingSystemVersion,
    device.isMobile,
    device.mobileDeviceBranding,
    device.flashVersion,
    device.javaEnabled,
    device.LANGUAGE,
    device.screenColors,
    device.screenResolution,
    device.deviceCategory,
    geoNetwork.country,
    geoNetwork.region,
    geoNetwork.metro,
    totals.bounces=1 AS isBouncedSession
  FROM
    {{ src('src_nested_export')}} )
SELECT
  *,
  LAG(sessionTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY sessionTimestamp) previousSessionTimestamp,
  TIMESTAMP_DIFF(sessionTimestamp,LAG(sessionTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY sessionTimestamp),MINUTE) minutesSinceLastVisitorSession
FROM
  sessions
