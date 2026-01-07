from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, max, min, current_timestamp, 
    date_format, hour, to_date, lit, when, round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime

def main():
    # 1. Initialisation de la SparkSession
    spark = SparkSession.builder \
        .appName("SmartCityAnalyticsZone") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Configuration du niveau de log
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 80)
    print("ÉTAPE 5 : STRUCTURATION ANALYTIQUE (ANALYTICS ZONE)")
    print("=" * 80)
    
    # 2. Lecture des données processed depuis HDFS
    processed_base_path = "hdfs://namenode:8020/data/processed/traffic"
    analytics_base_path = "hdfs://namenode:8020/data/analytics/traffic"
    
    print(f"\n>>> Lecture des données depuis : {processed_base_path}")
    
    try:
        # Lecture des différentes tables processed
        traffic_by_zone_df = spark.read.parquet(f"{processed_base_path}/traffic_by_zone")
        speed_by_road_df = spark.read.parquet(f"{processed_base_path}/speed_by_road")
        congestion_alerts_df = spark.read.parquet(f"{processed_base_path}/congestion_alerts")
        
        print(">>> Données processed chargées avec succès")
        print(f"   - Traffic par zone : {traffic_by_zone_df.count()} lignes")
        print(f"   - Vitesse par route : {speed_by_road_df.count()} lignes")
        print(f"   - Alertes de congestion : {congestion_alerts_df.count()} lignes")
        
    except Exception as e:
        print(f"ERREUR lors de la lecture des données processed : {e}")
        spark.stop()
        return
    
    # 3. Enrichissement et création de KPI analytiques
    
    print("\n>>> Création des KPI analytiques...")
    
    # A. Trafic par zone avec enrichissement
    analytics_traffic_by_zone = traffic_by_zone_df \
        .withColumn("avg_vehicle_count", round(col("avg_vehicle_count"), 2)) \
        .withColumn("avg_occupancy_rate", round(col("avg_occupancy_rate"), 2)) \
        .withColumn("traffic_level", 
                   when(col("avg_occupancy_rate") < 20, "FAIBLE")
                   .when(col("avg_occupancy_rate") < 50, "MODERÉ")
                   .when(col("avg_occupancy_rate") < 80, "ÉLEVÉ")
                   .otherwise("CRITIQUE")) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("zone"),
            col("avg_vehicle_count").alias("vehicle_count_avg"),
            col("avg_occupancy_rate").alias("occupancy_rate_avg"),
            col("traffic_level"),
            col("processed_date")
        )
    
    print(">>> KPI Trafic par zone créé")
    analytics_traffic_by_zone.show(truncate=False)
    
    # B. Vitesse par route avec classification
    analytics_speed_by_road = speed_by_road_df \
        .withColumn("avg_speed", round(col("avg_speed"), 2)) \
        .withColumn("speed_category",
                   when(col("avg_speed") > 80, "RAPIDE")
                   .when(col("avg_speed") > 50, "NORMALE")
                   .when(col("avg_speed") > 30, "LENTE")
                   .otherwise("TRÈS_LENTE")) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("road_id"),
            col("road_type"),
            col("avg_speed").alias("speed_avg_kmh"),
            col("speed_category"),
            col("processed_date")
        )
    
    print("\n>>> KPI Vitesse par route créé")
    analytics_speed_by_road.show(truncate=False)
    
    # C. Zones de congestion avec priorités
    analytics_congestion = congestion_alerts_df \
        .withColumn("global_congestion_rate", round(col("global_congestion_rate"), 2)) \
        .withColumn("congestion_priority",
                   when(col("global_congestion_rate") > 80, "URGENTE")
                   .when(col("global_congestion_rate") > 50, "HAUTE")
                   .when(col("global_congestion_rate") > 30, "MODERÉE")
                   .otherwise("FAIBLE")) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("zone"),
            col("global_congestion_rate").alias("congestion_rate_pct"),
            col("measurements_count"),
            col("status"),
            col("congestion_priority"),
            col("processed_date")
        )
    
    print("\n>>> KPI Congestion créé")
    analytics_congestion.show(truncate=False)
    
    # D. Vue consolidée : Résumé global de mobilité
    print("\n>>> Création d'une vue consolidée de mobilité...")
    
    # Jointure pour créer une vue globale
    mobility_summary = analytics_traffic_by_zone \
        .join(
            analytics_congestion.select("zone", "congestion_rate_pct", "congestion_priority"),
            on="zone",
            how="outer"
        ) \
        .withColumn("processed_date", current_timestamp()) \
        .withColumn("alert_status", 
                   when(col("congestion_rate_pct").isNotNull(), "ALERTE")
                   .otherwise("NORMAL"))
    
    print(">>> Vue consolidée créée")
    mobility_summary.show(truncate=False)
    
    # 4. Sauvegarde dans la zone Analytics avec partitionnement optimisé
    
    print(f"\n>>> Sauvegarde des données analytiques dans : {analytics_base_path}")
    print(">>> Format : Parquet avec compression Snappy")
    
    # Sauvegarde avec partitionnement par zone pour optimiser les requêtes analytiques
    # Le partitionnement par zone permet des requêtes plus rapides lors de l'analyse par zone
    
    try:
        # Ajouter une date d'exécution pour conserver l'historique
        run_date_expr = date_format(current_timestamp(), "yyyy-MM-dd")

        analytics_traffic_by_zone_d = analytics_traffic_by_zone.withColumn("run_date", run_date_expr)
        analytics_speed_by_road_d = analytics_speed_by_road.withColumn("run_date", run_date_expr)
        analytics_congestion_d = analytics_congestion.withColumn("run_date", run_date_expr)
        mobility_summary_d = mobility_summary.withColumn("run_date", run_date_expr)

        # A. Trafic par zone (partitionné par zone et date d'exécution)
        analytics_traffic_by_zone_d.write \
            .mode("append") \
            .partitionBy("zone", "run_date") \
            .parquet(f"{analytics_base_path}/traffic_by_zone")
        print("   ✓ Traffic par zone sauvegardé (partitionné par zone, run_date)")
        
        # B. Vitesse par route (partitionné par type de route et date d'exécution)
        analytics_speed_by_road_d.write \
            .mode("append") \
            .partitionBy("road_type", "run_date") \
            .parquet(f"{analytics_base_path}/speed_by_road")
        print("   ✓ Vitesse par route sauvegardée (partitionnée par road_type, run_date)")
        
        # C. Alertes de congestion (partitionné par date d'exécution)
        analytics_congestion_d.write \
            .mode("append") \
            .partitionBy("run_date") \
            .parquet(f"{analytics_base_path}/congestion_alerts")
        print("   ✓ Alertes de congestion sauvegardées (partitionnées par run_date)")
        
        # D. Vue consolidée (partitionnée par zone et date d'exécution)
        mobility_summary_d.write \
            .mode("append") \
            .partitionBy("zone", "run_date") \
            .parquet(f"{analytics_base_path}/mobility_summary")
        print("   ✓ Vue consolidée de mobilité sauvegardée (partitionnée par zone, run_date)")
        
    except Exception as e:
        print(f"ERREUR lors de la sauvegarde : {e}")
        spark.stop()
        return
    
    # 5. Vérification des fichiers créés
    print("\n>>> Vérification des fichiers analytics...")
    
    try:
        # Vérification de la structure
        verification_df = spark.read.parquet(f"{analytics_base_path}/traffic_by_zone")
        print(f"   ✓ Fichiers Parquet valides")
        print(f"   ✓ Nombre de zones dans analytics : {verification_df.select('zone').distinct().count()}")
        print(f"   ✓ Schéma vérifié :")
        verification_df.printSchema()
        
        # Aperçu des données
        print("\n>>> Aperçu des données analytics (premières lignes) :")
        verification_df.show(5, truncate=False)
        
    except Exception as e:
        print(f"ERREUR lors de la vérification : {e}")
    
    # 6. Résumé et justification du format analytique
    print("\n" + "=" * 80)
    print("RÉSUMÉ - JUSTIFICATION DU FORMAT ANALYTIQUE")
    print("=" * 80)
    print("""
    ✓ Format Parquet choisi pour :
      - Compression efficace : réduction de 80-90% vs JSON/CSV
      - Stockage columnar : lecture rapide des colonnes spécifiques
      - Support du partitionnement : requêtes optimisées par zone/route
      - Compatibilité : Spark, Hive, Presto, Pandas, etc.
      - Schéma intégré : validation automatique des données
    
    ✓ Partitionnement implémenté :
      - Par zone : optimise les requêtes d'analyse par zone géographique
      - Par road_type : optimise l'analyse par type de route
      - Améliore les performances de lecture de 10-100x
    
    ✓ KPI créés pour l'analyse :
      - Traffic par zone avec niveaux (FAIBLE, MODERÉ, ÉLEVÉ, CRITIQUE)
      - Vitesse par route avec catégories (RAPIDE, NORMALE, LENTE)
      - Congestion avec priorités (URGENTE, HAUTE, MOYENNE)
      - Vue consolidée pour analyse globale
    
    ✓ Optimisations :
      - Compression Snappy (bon compromis vitesse/compression)
      - Schéma structuré et typé
      - Timestamps pour traçabilité
    """)
    
    print(">>> Étape 5 terminée avec succès !")
    print("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()

