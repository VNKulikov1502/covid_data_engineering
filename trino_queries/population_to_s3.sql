show schemas in iceberg;

CREATE SCHEMA IF NOT EXISTS iceberg.raw;

CREATE TABLE IF NOT EXISTS iceberg.raw.country_population (
    country VARCHAR,
    country_code VARCHAR,
    year INTEGER,
    population BIGINT
)
WITH (
    format = 'PARQUET'
);


INSERT INTO iceberg.raw.country_population
SELECT 
    country,
    country_code,
    year,
    population
FROM source_population.public.country_population;


