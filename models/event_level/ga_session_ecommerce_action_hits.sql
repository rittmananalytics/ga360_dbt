WITH
  hits AS (
  SELECT
  CONCAT(fullVisitorId,CAST(visitId AS string)) AS session_id,
  TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64)) AS session_ts,
  SELECT TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64)) AS hit_ts,
  fullVisitorId AS full_visitor_id,
  hits.hitNumber as session_hit_number,
  hits.time as hit_time,
  hits.hour as hit_hour,
  hits.minute as hit_minute,
  hits.isSecure as hit_is_secure,
  hits.isInteraction as ehit_is_interaction,
  hits.isEntrance as hit_is_session_entrance,
  hits.isExit as hit_is_session_exit,
  hits.referer as hit_referer,
  hits.type as hit_type,
  hits.eventInfo.eventCategory,
  hits.eventInfo.eventAction,
  hits.eventInfo.eventLabel,
  hits.eventInfo.eventValue,
  case when hits.eCommerceAction.action_type = 1 then 'Click through of product lists'
         when hits.eCommerceAction.action_type = 2 then 'Product detail views'
         when hits.eCommerceAction.action_type = 3 then 'Add product(s) to cart'
         when hits.eCommerceAction.action_type = 4 then 'Remove product(s) from cart'
         when hits.eCommerceAction.action_type = 5 then 'Check out'
         when hits.eCommerceAction.action_type = 6 then 'Completed purchase'
         when hits.eCommerceAction.action_type = 7 then 'Refund of purchase'
         when hits.eCommerceAction.action_type = 8 then 'Checkout options'
         when hits.eCommerceAction.action_type = 0 then 'Unknown' end as ecommerce_action_type_desc,

  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` t,
    UNNEST(t.hits) AS hits,
    UNNEST(product) AS product)
SELECT
  *,
  LAG(hitTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY hitTimestamp) previousHitTimestamp,
  TIMESTAMP_DIFF(hitTimestamp,LAG(hitTimestamp,1) OVER (PARTITION BY fullVisitorId ORDER BY hitTimestamp),SECOND) secondsSinceLastVisitorHit
FROM
  allevents
