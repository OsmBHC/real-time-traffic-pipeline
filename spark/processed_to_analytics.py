from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round, current_timestamp
)

def main():
    spark = SparkSession.builder \
        .appName("SmartCityAnalyticsZoneBatch") \
        .master("local[*]") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    processed_base_path = "hdfs://namenode:8020/data/processed/traffic"
    analytics_base_path = "hdfs://namenode:8020/data/analytics/traffic"

    print("=" * 80)
    print("ÉTAPE 5 (BATCH) : STRUCTURATION ANALYTIQUE (ANALYTICS ZONE)")
    print("=" * 80)

    traffic_by_zone_df = spark.read.parquet(f"{processed_base_path}/traffic_by_zone")
    speed_by_road_df = spark.read.parquet(f"{processed_base_path}/speed_by_road")
    congestion_alerts_df = spark.read.parquet(f"{processed_base_path}/congestion_alerts")

    analytics_traffic_by_zone = traffic_by_zone_df \
        .withColumn("avg_vehicle_count", round(col("avg_vehicle_count"), 2)) \
        .withColumn("avg_occupancy_rate", round(col("avg_occupancy_rate"), 2)) \
        .withColumn(
            "traffic_level",
            when(col("avg_occupancy_rate") < 20, "FAIBLE")
            .when(col("avg_occupancy_rate") < 50, "MODERÉ")
            .when(col("avg_occupancy_rate") < 80, "ÉLEVÉ")
            .otherwise("CRITIQUE")
        ) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("zone"),
            col("avg_vehicle_count").alias("vehicle_count_avg"),
            col("avg_occupancy_rate").alias("occupancy_rate_avg"),
            col("traffic_level"),
            col("processed_date")
        )

    analytics_speed_by_road = speed_by_road_df \
        .withColumn("avg_speed", round(col("avg_speed"), 2)) \
        .withColumn(
            "speed_category",
            when(col("avg_speed") > 80, "RAPIDE")
            .when(col("avg_speed") > 50, "NORMALE")
            .when(col("avg_speed") > 30, "LENTE")
            .otherwise("TRÈS_LENTE")
        ) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("road_id"),
            col("road_type"),
            col("avg_speed").alias("speed_avg_kmh"),
            col("speed_category"),
            col("processed_date")
        )

    analytics_congestion = congestion_alerts_df \
        .withColumn("global_congestion_rate", round(col("global_congestion_rate"), 2)) \
        .withColumn(
            "congestion_priority",
            when(col("global_congestion_rate") > 80, "URGENTE")
            .when(col("global_congestion_rate") > 50, "HAUTE")
            .when(col("global_congestion_rate") > 30, "MODERÉE")
            .otherwise("FAIBLE")
        ) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("zone"),
            col("global_congestion_rate").alias("congestion_rate_pct"),
            col("measurements_count"),
            col("status"),
            col("congestion_priority"),
            col("processed_date")
        )

    mobility_summary = analytics_traffic_by_zone \
        .join(
            analytics_congestion.select("zone", "congestion_rate_pct", "congestion_priority"),
            on="zone",
            how="outer"
        ) \
        .withColumn("processed_date", current_timestamp()) \
        .withColumn(
            "alert_status",
            when(col("congestion_rate_pct") > 50, "ALERTE").otherwise("NORMAL")
        )

    analytics_traffic_by_zone.write.mode("overwrite").parquet(f"{analytics_base_path}/traffic_by_zone")
    analytics_speed_by_road.write.mode("overwrite").partitionBy("road_type").parquet(f"{analytics_base_path}/speed_by_road")
    analytics_congestion.write.mode("overwrite").parquet(f"{analytics_base_path}/congestion_alerts")
    mobility_summary.write.mode("overwrite").parquet(f"{analytics_base_path}/mobility_summary")

    print(">>> Analytics mis à jour avec succès.")
    spark.stop()

if __name__ == "__main__":
    main()