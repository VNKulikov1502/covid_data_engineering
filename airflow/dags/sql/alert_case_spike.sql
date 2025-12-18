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
    'CASE_RATE_POPULATION' AS alert_type,
    'HIGH' AS severity,
    t.new_cases_today AS metric_value,
    format(
        'COVID alert: %.3f%% of population infected today (%s new cases)',
        t.case_rate * 100,
        t.new_cases_today
    ) AS description
FROM (
    SELECT
        f.report_date,
        f.location_key,
        dl.country_name,
        dl.population,

        f.confirmed AS confirmed_today,
        LAG(f.confirmed) OVER (
            PARTITION BY f.location_key
            ORDER BY f.report_date
        ) AS confirmed_yesterday,

        f.confirmed
          - LAG(f.confirmed) OVER (
                PARTITION BY f.location_key
                ORDER BY f.report_date
            ) AS new_cases_today,

        CAST(
            f.confirmed
              - LAG(f.confirmed) OVER (
                    PARTITION BY f.location_key
                    ORDER BY f.report_date
                )
            AS DOUBLE
        ) / dl.population AS case_rate

    FROM iceberg.dds.fact_covid f
    JOIN iceberg.dds.dim_location dl
        ON f.location_key = dl.location_key
) t
WHERE t.report_date = DATE '{{ alert_date }}'
  AND t.confirmed_yesterday IS NOT NULL
  AND t.new_cases_today > 0
  AND t.population > 0
  AND t.case_rate >= 0.00005 -- 0,005% 
  AND NOT EXISTS (
      SELECT 1
      FROM alerts.alerts.covid_alerts a
      WHERE a.alert_date = t.report_date
        AND a.country = t.country_name
        AND a.alert_type = 'CASE_RATE_POPULATION'
  )