SELECT
fullVisitorId,
visitId,
visitNumber,
hitNumber,
pagePath
FROM
{{ ref('pageviews') }}
ORDER BY
fullVisitorId,
visitId,
visitNumber,
hitNumber
