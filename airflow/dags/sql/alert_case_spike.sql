INSERT INTO alerts.alerts.covid_alerts (
    alert_date,
    country,
    alert_type,
    severity,
    metric_value,
    description
)
SELECT
    report_date,
    country_region,
    'CASE_SPIKE',
    'HIGH',
    confirmed,
    format(
        'COVID case spike: %s today vs %s yesterday',
        confirmed,
        confirmed_yesterday
    )
FROM (
    SELECT
        report_date,
        country_region,
        confirmed,
        LAG(confirmed) OVER (
            PARTITION BY country_region
            ORDER BY report_date
        ) AS confirmed_yesterday
    FROM iceberg.ods.daily_country_stats
) t
WHERE report_date = DATE '{{ alert_date }}'
  AND confirmed_yesterday IS NOT NULL
  AND confirmed > confirmed_yesterday * 1.4
  AND confirmed > 1000
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_region
        AND a.alert_type = 'CASE_SPIKE'
  )