INSERT INTO alerts.alerts.covid_alerts (
    alert_date,
    country,
    alert_type,
    severity,
    metric_value,
    description
)
SELECT
    f.report_date,
    d.country_name,
    'INCIDENCE_100K',
    'MEDIUM',
    ((f.confirmed - f_yesterday.confirmed) * 100000.0 / d.population) AS incidence_per_100k,
    format(
        'Daily incidence: %.2f per 100k population',
        ((f.confirmed - f_yesterday.confirmed) * 100000.0 / d.population)
    )
FROM iceberg.dds.fact_covid f
JOIN iceberg.dds.dim_location d
    ON f.location_key = d.location_key
JOIN iceberg.dds.fact_covid f_yesterday
    ON f.location_key = f_yesterday.location_key
   AND f_yesterday.report_date = f.report_date - INTERVAL '1' DAY
WHERE f.report_date = DATE '{{ alert_date }}'
  AND ((f.confirmed - f_yesterday.confirmed) * 100000.0 / d.population) > 10
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = f.report_date
        AND a.country = d.country_name
        AND a.alert_type = 'INCIDENCE_100K'
  )