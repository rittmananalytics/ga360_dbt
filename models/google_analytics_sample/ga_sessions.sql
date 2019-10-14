{{ config( materialized='view') }}
select * from {{ source('google_analytics_sample', 'ga_sessions') }}
