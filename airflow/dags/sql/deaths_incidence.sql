INSERT INTO alerts.alerts.covid_alerts (
    alert_date,
    country,
    alert_type,
    severity,
    metric_value,
    description
)
SELECT
    t.report_date AS alert_date,
    t.country_name AS country,
    'DEATH_SPIKE_100K' AS alert_type,
    'HIGH' AS severity,
    t.deaths_per_100k AS metric_value,
    format(
        'High daily COVID mortality: %.2f per 100k population',
        t.deaths_per_100k
    ) AS description
FROM (
    SELECT
        f.report_date,
        f.location_key,
        dl.country_name,
        dl.population,

        f.deaths,
        LAG(f.deaths) OVER (
            PARTITION BY f.location_key
            ORDER BY f.report_date
        ) AS deaths_yesterday,

        CAST(f.deaths - LAG(f.deaths) OVER (
            PARTITION BY f.location_key
            ORDER BY f.report_date
        ) AS DOUBLE) * 100000.0 / dl.population AS deaths_per_100k

    FROM iceberg.dds.fact_covid f
    JOIN iceberg.dds.dim_location dl
        ON f.location_key = dl.location_key
) t
WHERE t.report_date = DATE '{{ alert_date }}'
  AND t.deaths_yesterday IS NOT NULL
  AND t.deaths_per_100k > 1
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_name
        AND a.alert_type = 'DEATH_SPIKE_100K'
  )