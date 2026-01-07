import json
import random
from datetime import datetime
from collections import defaultdict

from kafka import KafkaConsumer
from hdfs import InsecureClient
from hdfs.util import HdfsError  # Pour gérer les erreurs spécifiques

# ================= CONFIGURATION =================
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
KAFKA_TOPIC = 'traffic-events'
KAFKA_GROUP_ID = 'hdfs-consumer-group'

HDFS_NAMENODE_URL = 'http://localhost:9870'  # Depuis l'hôte !
HDFS_USER = 'root'
RAW_BASE_PATH = '/data/raw/traffic'

BATCH_SIZE = 50      # Écrire toutes les 50 messages
WRITE_TIMEOUT = 30   # Ou toutes les 30 secondes max

# ================= CONSUMER & HDFS CLIENT =================
def get_hdfs_client():
    return InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)

def get_partition_path(event):
    event_time = datetime.fromisoformat(event['event_time'])
    date = event_time.strftime('%Y-%m-%d')
    zone = event['zone'].replace(' ', '_')
    return f"{RAW_BASE_PATH}/date={date}/zone={zone}"

def ensure_directory(client, path):
    """Crée le dossier de manière idempotente"""
    try:
        client.status(path)
    except HdfsError:
        client.makedirs(path)

def main():
    print("Démarrage du consumer Kafka → HDFS...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest',  # Change en 'earliest' pour reconsommer l'historique
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"Connecté au topic '{KAFKA_TOPIC}'")

    hdfs_client = get_hdfs_client()

    batch = defaultdict(list)  # {partition_path: [json_lines]}
    last_write = datetime.now()

    try:
        for message in consumer:
            event = message.value
            partition_path = get_partition_path(event)
            batch[partition_path].append(json.dumps(event))

            # Flush si batch trop gros ou timeout
            if sum(len(lines) for lines in batch.values()) >= BATCH_SIZE or \
               (datetime.now() - last_write).seconds >= WRITE_TIMEOUT:
                flush_batch(hdfs_client, batch)
                batch.clear()
                last_write = datetime.now()

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    except Exception as e:
        print(f"Erreur inattendue : {e}")
    finally:
        if batch:
            flush_batch(hdfs_client, batch)
        consumer.close()

def flush_batch(client, batch):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    for partition_path, lines in batch.items():
        ensure_directory(client, partition_path)
        
        # Nom de fichier unique par flush
        filename = f"{partition_path}/events_{timestamp}_{random.randint(0, 9999)}.jsonl"
        
        try:
            with client.write(filename, encoding='utf-8') as writer:
                for line in lines:
                    writer.write(line + '\n')
            print(f"Écrit {len(lines)} événements → {filename}")
        except Exception as e:
            print(f"Erreur écriture {filename} : {e}")

if __name__ == "__main__":
    main()