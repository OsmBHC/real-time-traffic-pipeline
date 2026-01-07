from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, current_timestamp, window, lit, date_format, when

def main():
    # 1. Initialisation de la SparkSession
    spark = SparkSession.builder \
        .appName("SmartCityTrafficAnalysis") \
        .master("local[*]") \
        .getOrCreate()
    
    # Configuration du niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")
    
    print(">>> Démarrage du traitement Spark...")

    # 2. Lecture des données brutes depuis HDFS
    # HDFS path interne au cluster Docker
    raw_data_path = "hdfs://namenode:8020/data/raw/traffic"
    
    try:
        # Lecture récursive avec support du schema inference
        df = spark.read.json(raw_data_path)
        print(f">>> Données chargées avec succès. Schéma :")
        df.printSchema()
        
    except Exception as e:
        print(f"Erreur lors de la lecture des données : {e}")
        return

    # 3. Traitement et Calcul des Indicateurs
    
    # A. Trafic moyen par zone
    traffic_by_zone = df.groupBy("zone") \
        .agg(
            avg("vehicle_count").alias("avg_vehicle_count"),
            avg("occupancy_rate").alias("avg_occupancy_rate")
        )
    
    print(">>> Trafic moyen par zone :")
    traffic_by_zone.show()

    # B. Vitesse moyenne par route
    speed_by_road = df.groupBy("road_id", "road_type") \
        .agg(
            avg("average_speed").alias("avg_speed")
        )
    
    print(">>> Vitesse moyenne par route :")
    speed_by_road.show()

    # C. Taux de congestion avec catégorisation des niveaux
    # Catégories harmonisées avec l'étape analytics
    congestion_zones = df.groupBy("zone") \
        .agg(
            avg("occupancy_rate").alias("global_congestion_rate"),
            count("*").alias("measurements_count")
        ) \
        .withColumn(
            "status",
            when(col("global_congestion_rate") > 80, "CRITICAL CONGESTION")
            .when(col("global_congestion_rate") > 50, "HIGH CONGESTION")
            .when(col("global_congestion_rate") > 30, "MODERATE CONGESTION")
            .otherwise("LOW CONGESTION")
        )
    
    print(">>> Zones à forte congestion :")
    congestion_zones.show()

    # 4. Sauvegarde des résultats (Data Lake - Processed Zone)
    # On sauvegarde au format Parquet pour l'étape 5 (Analytics Zone)
    processed_base_path = "hdfs://namenode:8020/data/processed/traffic"
    
    print(">>> Sauvegarde des résultats dans HDFS...")
    
    # Ajouter une date d'exécution pour conserver l'historique des runs
    run_date_expr = date_format(current_timestamp(), "yyyy-MM-dd")

    traffic_by_zone_with_date = traffic_by_zone.withColumn("run_date", run_date_expr)
    speed_by_road_with_date = speed_by_road.withColumn("run_date", run_date_expr)
    congestion_zones_with_date = congestion_zones.withColumn("run_date", run_date_expr)

    traffic_by_zone_with_date.write \
        .mode("append") \
        .partitionBy("run_date") \
        .parquet(f"{processed_base_path}/traffic_by_zone")

    speed_by_road_with_date.write \
        .mode("append") \
        .partitionBy("run_date") \
        .parquet(f"{processed_base_path}/speed_by_road")

    congestion_zones_with_date.write \
        .mode("append") \
        .partitionBy("run_date") \
        .parquet(f"{processed_base_path}/congestion_alerts")
    
    print(">>> Traitement terminé avec succès.")
    spark.stop()

if __name__ == "__main__":
    main()
