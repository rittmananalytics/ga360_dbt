WITH
  countryHier AS (
  SELECT
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country
  FROM
    {{ src('src_nested_export') }} )
  GROUP BY 1,2,3
SELECT
  country,
  subContinent,
  continent
FROM
  countryHier
