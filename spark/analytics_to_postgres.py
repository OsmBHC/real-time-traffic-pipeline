from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
import sys

def main():
    """
    Étape 6 - Préparation des données pour Grafana
    Exporte les données analytics depuis HDFS (Parquet) vers PostgreSQL
    pour permettre la visualisation dans Grafana.
    """
    
    # 1. Initialisation de la SparkSession avec support JDBC
    # Utilisation du JAR PostgreSQL local au lieu de téléchargement Maven
    postgresql_jar_path = "/opt/spark-jobs/jars/postgresql-42.7.1.jar"
    
    spark = SparkSession.builder \
        .appName("AnalyticsToPostgreSQL") \
        .master("local[*]") \
        .config("spark.jars", postgresql_jar_path) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 80)
    print("ÉTAPE 6 : EXPORT VERS POSTGRESQL POUR GRAFANA")
    print("=" * 80)
    
    # 2. Configuration PostgreSQL
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
    
    # 3. Lecture des données analytics depuis HDFS
    analytics_base_path = "hdfs://namenode:8020/data/analytics/traffic"
    
    print(f"\n>>> Lecture des données analytics depuis : {analytics_base_path}")
    
    try:
        # Lecture des tables analytics
        traffic_by_zone_df = spark.read.parquet(f"{analytics_base_path}/traffic_by_zone")
        speed_by_road_df = spark.read.parquet(f"{analytics_base_path}/speed_by_road")
        congestion_alerts_df = spark.read.parquet(f"{analytics_base_path}/congestion_alerts")
        mobility_summary_df = spark.read.parquet(f"{analytics_base_path}/mobility_summary")
        
        print(">>> Données analytics chargées avec succès")
        print(f"   - Traffic par zone : {traffic_by_zone_df.count()} lignes")
        print(f"   - Vitesse par route : {speed_by_road_df.count()} lignes")
        print(f"   - Alertes de congestion : {congestion_alerts_df.count()} lignes")
        print(f"   - Vue consolidée : {mobility_summary_df.count()} lignes")
        
    except Exception as e:
        print(f"ERREUR lors de la lecture des données analytics : {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        return
    
    # 4. Préparation des données pour PostgreSQL
    print("\n>>> Préparation des données pour PostgreSQL...")
    
    # A. Trafic par zone
    traffic_for_grafana = traffic_by_zone_df \
        .withColumn("timestamp", current_timestamp()) \
        .select(
            col("zone"),
            col("vehicle_count_avg"),
            col("occupancy_rate_avg"),
            col("traffic_level"),
            col("timestamp")
        )
    
    # B. Vitesse par route
    speed_for_grafana = speed_by_road_df \
        .withColumn("timestamp", current_timestamp()) \
        .select(
            col("road_id"),
            col("road_type"),
            col("speed_avg_kmh"),
            col("speed_category"),
            col("timestamp")
        )
    
    # C. Congestion
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
    
    # D. Vue consolidée
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
    
    # 5. Écriture dans PostgreSQL (mode append pour l'historique)
    print("\n>>> Écriture des données dans PostgreSQL...")
    
    try:
        # A. Trafic par zone
        print("   → Export: traffic_by_zone")
        traffic_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "traffic_by_zone", properties=properties)
        print("     ✓ Table traffic_by_zone mise à jour")
        
        # B. Vitesse par route
        print("   → Export: speed_by_road")
        speed_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "speed_by_road", properties=properties)
        print("     ✓ Table speed_by_road mise à jour")
        
        # C. Congestion
        print("   → Export: congestion_alerts")
        congestion_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "congestion_alerts", properties=properties)
        print("     ✓ Table congestion_alerts mise à jour")
        
        # D. Vue consolidée
        print("   → Export: mobility_summary")
        summary_for_grafana.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(jdbc_url, "mobility_summary", properties=properties)
        print("     ✓ Table mobility_summary mise à jour")
        
    except Exception as e:
        print(f"ERREUR lors de l'export vers PostgreSQL : {e}")
        print("\nAssurez-vous que :")
        print("  1. PostgreSQL est démarré et accessible")
        print("  2. Les tables existent (elles seront créées automatiquement)")
        print("  3. Le driver PostgreSQL est disponible")
        import traceback
        traceback.print_exc()
        spark.stop()
        return
    
    # 6. Vérification
    print("\n>>> Vérification des données dans PostgreSQL...")
    
    try:
        # Vérification des tables
        test_df = spark.read.jdbc(jdbc_url, "traffic_by_zone", properties=properties)
        print(f"   ✓ Traffic par zone : {test_df.count()} lignes")
        print("\n   Aperçu des données :")
        test_df.show(5, truncate=False)
        
    except Exception as e:
        print(f"   ✗ Erreur lors de la vérification : {e}")
    
    print("\n>>> Export terminé avec succès !")
    print(">>> Les données sont maintenant disponibles dans PostgreSQL pour Grafana")
    print("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()

