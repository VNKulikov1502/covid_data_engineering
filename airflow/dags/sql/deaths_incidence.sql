INSERT INTO alerts.alerts.covid_alerts (
    alert_date,
    country,
    alert_type,
    severity,
    metric_value,
    description
)
SELECT
    t.report_date,
    t.country_region,
    'DEATH_SPIKE_100K',
    'HIGH',
    ((t.deaths - t.deaths_yesterday) * 100000.0 / d.population),
    format(
        'High daily COVID mortality: %s per 100k population',
        ((t.deaths - t.deaths_yesterday) * 100000.0 / d.population)
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
JOIN iceberg.dds.dim_location d
    ON t.country_region = d.country_name
WHERE t.report_date = DATE '{{ alert_date }}'
  AND t.deaths_yesterday IS NOT NULL
  AND (t.deaths - t.deaths_yesterday) * 100000.0 / d.population > 1
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_region
        AND a.alert_type = 'DEATH_SPIKE_100K'
  )