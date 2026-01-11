from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.errors import AnalysisException
import sys

def main():
    postgresql_jar_path = "/opt/spark-jobs/jars/postgresql-42.7.1.jar"
    
    spark = SparkSession.builder \
        .appName("AnalyticsToPostgreSQLBatch") \
        .master("local[*]") \
        .config("spark.jars", postgresql_jar_path) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    
    print("=" * 80)
    print("ÉTAPE 6 : EXPORT VERS POSTGRESQL POUR GRAFANA")
    print("=" * 80)
    
    postgres_host = "postgres"
    postgres_port = "5432"
    postgres_db = "traffic"
    postgres_user = "traffic_user"
    postgres_password = "traffic_password"
    
    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
    }
    
    analytics_base_path = "hdfs://namenode:8020/data/analytics/traffic"

    # Define schemas
    traffic_schema = StructType([
        StructField("zone", StringType()),
        StructField("vehicle_count_avg", DoubleType()),
        StructField("occupancy_rate_avg", DoubleType()),
        StructField("traffic_level", StringType()),
        StructField("processed_date", TimestampType())
    ])

    speed_schema = StructType([
        StructField("road_id", StringType()),
        StructField("road_type", StringType()),
        StructField("speed_avg_kmh", DoubleType()),
        StructField("speed_category", StringType()),
        StructField("processed_date", TimestampType())
    ])

    congestion_schema = StructType([
        StructField("zone", StringType()),
        StructField("congestion_rate_pct", DoubleType()),
        StructField("measurements_count", IntegerType()),
        StructField("status", StringType()),
        StructField("congestion_priority", StringType()),
        StructField("processed_date", TimestampType())
    ])

    summary_schema = StructType([
        StructField("zone", StringType()),
        StructField("vehicle_count_avg", DoubleType()),
        StructField("occupancy_rate_avg", DoubleType()),
        StructField("traffic_level", StringType()),
        StructField("congestion_rate_pct", DoubleType()),
        StructField("congestion_priority", StringType()),
        StructField("alert_status", StringType()),
        StructField("processed_date", TimestampType())
    ])

    print(f"\n>>> Export batch vers PostgreSQL depuis : {analytics_base_path}")

    def safe_read_parquet(path, schema):
        try:
            return spark.read.option("ignoreMissingFiles", "true").parquet(path)
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e) or "UNABLE_TO_INFER_SCHEMA" in str(e):
                return spark.createDataFrame([], schema)
            raise

    traffic_by_zone_df = safe_read_parquet(f"{analytics_base_path}/traffic_by_zone", traffic_schema)
    speed_by_road_df = safe_read_parquet(f"{analytics_base_path}/speed_by_road", speed_schema)
    congestion_alerts_df = safe_read_parquet(f"{analytics_base_path}/congestion_alerts", congestion_schema)
    mobility_summary_df = safe_read_parquet(f"{analytics_base_path}/mobility_summary", summary_schema)

    if all(df.rdd.isEmpty() for df in [traffic_by_zone_df, speed_by_road_df, congestion_alerts_df, mobility_summary_df]):
        print("No data to export")
        spark.stop()
        return

    traffic_for_grafana = traffic_by_zone_df \
        .withColumn("timestamp", current_timestamp()) \
        .select(
            col("zone"),
            col("vehicle_count_avg"),
            col("occupancy_rate_avg"),
            col("traffic_level"),
            col("timestamp")
        )

    speed_for_grafana = speed_by_road_df \
        .withColumn("timestamp", current_timestamp()) \
        .select(
            col("road_id"),
            col("road_type"),
            col("speed_avg_kmh"),
            col("speed_category"),
            col("timestamp")
        )

    congestion_for_grafana = congestion_alerts_df \
        .withColumn("timestamp", current_timestamp()) \
        .select(
            col("zone"),
            col("congestion_rate_pct"),
            col("measurements_count"),
            col("status"),
            col("congestion_priority"),
            col("timestamp")
        )

    summary_for_grafana = mobility_summary_df \
        .withColumn("timestamp", current_timestamp()) \
        .select(
            col("zone"),
            col("vehicle_count_avg"),
            col("occupancy_rate_avg"),
            col("traffic_level"),
            col("congestion_rate_pct"),
            col("congestion_priority"),
            col("alert_status"),
            col("timestamp")
        )

    try:
        traffic_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "traffic_by_zone", properties=properties)

        traffic_for_grafana.write \
            .mode("append") \
            .jdbc(jdbc_url, "traffic_by_zone_ts", properties=properties)

        speed_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "speed_by_road", properties=properties)

        speed_for_grafana.write \
            .mode("append") \
            .jdbc(jdbc_url, "speed_by_road_ts", properties=properties)

        congestion_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "congestion_alerts", properties=properties)

        congestion_for_grafana.write \
            .mode("append") \
            .jdbc(jdbc_url, "congestion_alerts_ts", properties=properties)

        summary_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "mobility_summary", properties=properties)

        summary_for_grafana.write \
            .mode("append") \
            .jdbc(jdbc_url, "mobility_summary_ts", properties=properties)

        print("✓ PostgreSQL mis à jour")
    except Exception as e:
        print(f"ERREUR export PostgreSQL : {e}")

    spark.stop()

if __name__ == "__main__":
    main()