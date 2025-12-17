import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: process_covid_data_mart.py <REPORT_DATE>")

    report_date = sys.argv[1]
    print(f"--- Processing Data Mart for date: {report_date} ---")

    spark = SparkSession.builder \
        .appName("Build Covid Data Mart for Superset") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.data_mart")

    ddl = """
    CREATE TABLE IF NOT EXISTS iceberg.data_mart.covid_analytics (
        report_date DATE,
        country_name STRING,
        population LONG,
        total_confirmed LONG,
        total_deaths LONG,
        total_recovered LONG,
        current_active_cases LONG,
        new_cases_today LONG,
        new_deaths_today LONG,
        cases_per_100k LONG,
        fatality_rate_percent DOUBLE,
        recovery_rate_percent DOUBLE,
        risk_category STRING
    )
    USING iceberg
    PARTITIONED BY (report_date)
    TBLPROPERTIES ('format-version'='2')
    """
    spark.sql(ddl)

    sql_query = f"""
    WITH raw_joined AS (
        SELECT 
            f.report_date,
            d.country_name,
            d.population,
            f.confirmed AS total_confirmed,
            f.deaths AS total_deaths,
            f.recovered AS total_recovered,
            (f.confirmed - f.deaths - f.recovered) AS current_active_cases
        FROM iceberg.dds.fact_covid f
        INNER JOIN iceberg.dds.dim_location d ON f.location_key = d.location_key
        WHERE d.country_name IS NOT NULL 
          AND d.population IS NOT NULL 
          AND d.population > 0
    ),
    
    calc_deltas AS (
        SELECT 
            *,
            GREATEST(
                COALESCE(total_confirmed - LAG(total_confirmed, 1) OVER (PARTITION BY country_name ORDER BY report_date), 0), 
                0
            ) as new_cases_today,
            
            GREATEST(
                COALESCE(total_deaths - LAG(total_deaths, 1) OVER (PARTITION BY country_name ORDER BY report_date), 0),
                0
            ) as new_deaths_today
        FROM raw_joined
    ),
    
    calc_advanced AS (
        SELECT
            *,
            CAST(ROUND((total_confirmed / population) * 100000, 0) AS LONG) as cases_per_100k,
            
            CASE WHEN total_confirmed > 0 THEN ROUND((total_deaths / total_confirmed) * 100, 2) ELSE 0 END as fatality_rate_percent,
            CASE WHEN total_confirmed > 0 THEN ROUND((total_recovered / total_confirmed) * 100, 2) ELSE 0 END as recovery_rate_percent
        FROM calc_deltas
    )

    SELECT 
        report_date,
        country_name,
        population,
        total_confirmed,
        total_deaths,
        total_recovered,
        current_active_cases,
        new_cases_today,
        new_deaths_today,
        cases_per_100k,
        fatality_rate_percent,
        recovery_rate_percent,
        
        CASE 
            WHEN cases_per_100k > 5000 THEN 'Critical'
            WHEN cases_per_100k > 1000 THEN 'High'
            WHEN cases_per_100k > 100 THEN 'Medium'
            ELSE 'Low'
        END as risk_category
        
    FROM calc_advanced
    WHERE report_date = DATE '{report_date}'
    """

    print("Executing Analytics Calculation...")
    df_result = spark.sql(sql_query)

    target_table = "iceberg.data_mart.covid_analytics"
    print(f"Overwriting ONLY partition {report_date} in {target_table}...")

    df_result.writeTo(target_table) \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .overwritePartitions()

    print(f"Data Mart: Partition {report_date} successfully updated. History preserved.")

    spark.stop()

if __name__ == "__main__":
    main()