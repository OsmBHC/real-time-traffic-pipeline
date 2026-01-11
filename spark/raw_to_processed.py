from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp, when, round

def main():
    spark = SparkSession.builder \
        .appName("SmartCityTrafficRawToProcessedBatch") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_data_path = "hdfs://namenode:8020/data/raw/traffic"
    processed_base_path = "hdfs://namenode:8020/data/processed/traffic"
    print(">>> Démarrage du traitement Spark (BATCH)...")
    print(f">>> Lecture batch (recursive) depuis: {raw_data_path}")

    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json(raw_data_path)

    traffic_by_zone = df.groupBy("zone") \
        .agg(
            avg("vehicle_count").alias("avg_vehicle_count"),
            avg("occupancy_rate").alias("avg_occupancy_rate")
        ) \
        .withColumn("avg_vehicle_count", round(col("avg_vehicle_count"), 2)) \
        .withColumn("avg_occupancy_rate", round(col("avg_occupancy_rate"), 2)) \
        .withColumn("processed_date", current_timestamp())

    speed_by_road = df.groupBy("road_id", "road_type") \
        .agg(avg("average_speed").alias("avg_speed")) \
        .withColumn("avg_speed", round(col("avg_speed"), 2)) \
        .withColumn("processed_date", current_timestamp())

    congestion_zones = df.groupBy("zone") \
        .agg(
            avg("occupancy_rate").alias("global_congestion_rate"),
            count("*").alias("measurements_count")
        ) \
        .withColumn("global_congestion_rate", round(col("global_congestion_rate"), 2)) \
        .withColumn(
            "status",
            when(col("global_congestion_rate") > 80, "CRITICAL CONGESTION")
            .when(col("global_congestion_rate") > 50, "HIGH CONGESTION")
            .otherwise("MODERATE CONGESTION")
        ) \
        .withColumn("processed_date", current_timestamp())

    traffic_by_zone.write.mode("overwrite").parquet(f"{processed_base_path}/traffic_by_zone")
    speed_by_road.write.mode("overwrite").parquet(f"{processed_base_path}/speed_by_road")
    congestion_zones.write.mode("overwrite").parquet(f"{processed_base_path}/congestion_alerts")

    print(">>> Traitement terminé avec succès.")
    spark.stop()

if __name__ == "__main__":
    main()