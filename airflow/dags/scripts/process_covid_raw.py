import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, coalesce, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType

TARGET_ICEBERG_SCHEMA = StructType([
    StructField("FIPS", StringType(), True),
    StructField("Admin2", StringType(), True),
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Last_Update", TimestampType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long_", DoubleType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("Combined_Key", StringType(), True),
    StructField("Incident_Rate", DoubleType(), True),
    StructField("Case_Fatality_Ratio", DoubleType(), True)
])

def normalize_to_target_schema(df, schema):
    for c in df.columns:
        clean_name = c.strip().replace("/", "_").replace(" ", "_").replace("-", "_")
        if c != clean_name:
            df = df.withColumnRenamed(c, clean_name)

    selected_cols = []
    for field in schema.fields:
        target_col_name = field.name
        target_data_type = field.dataType

        found_col = None

        if target_col_name in df.columns:
            found_col = col(target_col_name)
        elif target_col_name == "Lat" and "Latitude" in df.columns:
            found_col = col("Latitude")
        elif target_col_name == "Long_" and "Longitude" in df.columns:
            found_col = col("Longitude")
        elif target_col_name == "Province_State" and "Province_State" not in df.columns and "Province/State" in df.columns:
            found_col = col("Province/State")

        if found_col is not None:
            if target_col_name == "Last_Update":
                final_col = coalesce(
                    to_timestamp(found_col),
                    to_timestamp(found_col, "M/d/yyyy H:mm"),
                    to_timestamp(found_col, "yyyy-MM-dd HH:mm:ss")
                )
            else:
                final_col = found_col.cast(target_data_type)
        else:
            final_col = lit(None).cast(target_data_type)

        selected_cols.append(final_col.alias(target_col_name))

    return df.select(*selected_cols)


def main():
    spark = SparkSession.builder \
        .appName("Covid Processing S3") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.hive.metastore.client.capability.check", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    if len(sys.argv) < 3:
        raise ValueError("Usage: process_covid_s3.py <S3_INPUT_PATH> <TARGET_TABLE>")

    s3_input_path = sys.argv[1]
    target_table = sys.argv[2]

    print(f"Processing {s3_input_path} -> {target_table}")

    try:
        df_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(s3_input_path)

        df_normalized = normalize_to_target_schema(df_raw, TARGET_ICEBERG_SCHEMA)

        df_final = df_normalized \
            .withColumn("source_file", lit(s3_input_path)) \
            .withColumn("ingestion_ts", current_timestamp())

        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.raw")

        writer = df_final.sortWithinPartitions("Country_Region").writeTo(target_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .partitionedBy(col("Country_Region"))

        try:
            spark.table(target_table)
            print(f"Appending to {target_table}...")
            writer.append()
        except:
            print(f"Creating {target_table}...")
            writer.create()

        print("SUCCESS")

    except Exception as e:
        print("ERROR")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()