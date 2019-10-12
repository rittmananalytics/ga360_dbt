with browsers as
(SELECT
    concat(concat(device.browser,' - '),device.operatingSystem) as browser_os,
    device.browser,
    device.operatingSystem,
    device.isMobile,
    device.deviceCategory FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
    group by 1,2,3,4,5)
SELECT
    browser_os,
    browser,
    operatingSystem,
    isMobile,
    deviceCategory
FROM browsers
