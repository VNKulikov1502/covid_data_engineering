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
    'DEATH_RATE_POPULATION' AS alert_type,
    'HIGH' AS severity,
    t.new_deaths_today AS metric_value,
    format(
        'COVID death alert: %.5f%% of population died today (%s new deaths)',
        t.death_rate * 100,
        t.new_deaths_today
    ) AS description
FROM (
    SELECT
        f.report_date,
        f.location_key,
        dl.country_name,
        dl.population,

        f.deaths AS deaths_today,
        LAG(f.deaths) OVER (
            PARTITION BY f.location_key
            ORDER BY f.report_date
        ) AS deaths_yesterday,

        f.deaths
          - LAG(f.deaths) OVER (
                PARTITION BY f.location_key
                ORDER BY f.report_date
            ) AS new_deaths_today,

        CAST(
            f.deaths
              - LAG(f.deaths) OVER (
                    PARTITION BY f.location_key
                    ORDER BY f.report_date
                )
            AS DOUBLE
        ) / dl.population AS death_rate

    FROM iceberg.dds.fact_covid f
    JOIN iceberg.dds.dim_location dl
        ON f.location_key = dl.location_key
) t
WHERE t.report_date = DATE '{{ alert_date }}'
  AND t.deaths_yesterday IS NOT NULL
  AND t.new_deaths_today > 0
  AND t.population > 0
  AND t.death_rate >= 0.0000005
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_name
        AND a.alert_type = 'DEATH_RATE_POPULATION'
  )