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
    'INCIDENCE_100K' AS alert_type,
    'MEDIUM' AS severity,
    t.incidence_per_100k AS metric_value,
    format(
        'Daily incidence: %.2f per 100k population',
        t.incidence_per_100k
    ) AS description
FROM (
    SELECT
        f.report_date,
        f.location_key,
        dl.country_name,
        dl.population,

        f.confirmed,
        LAG(f.confirmed) OVER (
            PARTITION BY f.location_key
            ORDER BY f.report_date
        ) AS confirmed_yesterday,

        CAST(f.confirmed - LAG(f.confirmed) OVER (
            PARTITION BY f.location_key
            ORDER BY f.report_date
        ) AS DOUBLE) * 100000.0 / dl.population AS incidence_per_100k

    FROM iceberg.dds.fact_covid f
    JOIN iceberg.dds.dim_location dl
        ON f.location_key = dl.location_key
) t
WHERE t.report_date = DATE '{{ alert_date }}'
  AND t.confirmed_yesterday IS NOT NULL
  AND t.incidence_per_100k > 10
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_name
        AND a.alert_type = 'INCIDENCE_100K'
  )