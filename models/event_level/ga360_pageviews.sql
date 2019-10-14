WITH
  pageviews AS (
  SELECT
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS session_id,
    TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64)) AS session_ts,
    SELECT TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64)) AS pageview_ts,
    fullVisitorId AS full_visitor_id,
    hits.hitNumber as pageview_session_event_number,
    hits.time as pageview_time,
    hits.hour as pageview_hour,
    hits.minute as pageview_minute,
    hits.isSecure as pageview_is_secure,
    hits.isInteraction as pageview_is_interaction,
    hits.isEntrance as pageview_is_session_entrance,
    hits.isExit as pageview_is_session_exit,
    hits.referer as pageview_referer,
    hits.page.pageTitle as pageview_page_title,
    hits.page.searchKeyword as pageview_search_keyword,
    hits.page.searchCategory as pageview_search_category,
    hits.page.pagePathLevel1 as pageview_page_path_level1,
    hits.page.pagePathLevel2 as pageview_page_path_level2,
    hits.page.pagePathLevel3 as pageview_page_path_level3,
    hits.page.pagePathLevel4 as pageview_page_path_level4,
    hits.page.pagePath as pageview_page_path,
    hits.page.hostname as pageview_hostname
  FROM
    {{ ref('google_analytics_sample')}}` t,
    UNNEST (t.hits) AS hits)
SELECT
  *
FROM
  pageviews
