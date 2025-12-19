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
    'DEATH_SPIKE',
    'CRITICAL',
    deaths,
    format(
        'COVID death spike: %s today vs %s yesterday',
        deaths,
        deaths_yesterday
    )
FROM (
    SELECT
        report_date,
        country_region,
        deaths,
        LAG(deaths) OVER (
            PARTITION BY country_region
            ORDER BY report_date
        ) AS deaths_yesterday
    FROM iceberg.ods.daily_country_stats
) t
WHERE report_date = DATE '{{ alert_date }}'
  AND deaths_yesterday IS NOT NULL
  AND deaths > deaths_yesterday * 1.3
  AND deaths >= 50
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_region
        AND a.alert_type = 'DEATH_SPIKE'
  )