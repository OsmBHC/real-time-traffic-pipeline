import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

# =================CONFIGURATION=================
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
KAFKA_TOPIC = 'traffic-events'

ZONES = ['Centre Ville', 'Zone Industrielle', 'Quartier Residentiel', 'Aeroport']
ROAD_TYPES = {
    'Autoroute': {'max_speed': 130, 'capacity': 100},
    'Avenue': {'max_speed': 60, 'capacity': 50},
    'Rue': {'max_speed': 30, 'capacity': 20}
}
ROADS = [
    {'id': 'A1', 'type': 'Autoroute'},
    {'id': 'A86', 'type': 'Autoroute'},
    {'id': 'Av. des Champs', 'type': 'Avenue'},
    {'id': 'Rue de la Republique', 'type': 'Rue'},
    {'id': 'Bd de la Resistance', 'type': 'Avenue'}
]

class TrafficSensor:
    def __init__(self, road_id, road_type, zone):
        self.sensor_id = str(uuid.uuid4())[:8]
        self.road_id = road_id
        self.road_type = road_type
        self.zone = zone
        self.max_speed = ROAD_TYPES[road_type]['max_speed']
        self.capacity = ROAD_TYPES[road_type]['capacity']

    def get_time_factor(self):
        """Retourne un facteur de trafic basé sur l'heure (Simulation heures de pointe)"""
        now = datetime.now()
        hour = now.hour
        
        # Heures de pointe : 7h-9h et 17h-19h
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            return random.uniform(0.8, 1.0) # Trafic très dense
        elif 10 <= hour <= 16:
            return random.uniform(0.4, 0.7) # Trafic moyen
        else:
            return random.uniform(0.05, 0.3) # Trafic calme (nuit)

    def generate_event(self):
        traffic_load = self.get_time_factor()
        
        # Ajout d'un peu d'aléatoire sur le trafic
        traffic_load += random.uniform(-0.1, 0.1)
        traffic_load = max(0, min(1, traffic_load))  # Borner entre 0 et 1

        # Calcul des métriques
        vehicle_count = int(self.capacity * traffic_load)
        occupancy_rate = round(traffic_load * 100, 2)

        # Définir la vitesse selon l'occupation
        if self.road_type == "Autoroute":
            if occupancy_rate >= 90:  # Congestion critique
                speed = random.uniform(20, 50)
            elif occupancy_rate >= 70:  # Trafic dense
                speed = random.uniform(50, 80)
            else:  # Trafic fluide
                speed = random.uniform(80, 140)
        elif self.road_type in ["Avenue", "Boulevard"]:
            if occupancy_rate >= 90:
                speed = random.uniform(5, 25)
            elif occupancy_rate >= 70:
                speed = random.uniform(20, 40)
            else:
                speed = random.uniform(40, 60)
        elif self.road_type == "Rue":
            if occupancy_rate >= 90:
                speed = random.uniform(2, 10)
            elif occupancy_rate >= 70:
                speed = random.uniform(10, 20)
            else:
                speed = random.uniform(20, 30)
        else:
            speed = self.max_speed * (1 - traffic_load)


        average_speed = round(speed, 1)

        event = {
            "sensor_id": self.sensor_id,
            "road_id": self.road_id,
            "road_type": self.road_type,
            "zone": self.zone,
            "vehicle_count": vehicle_count,
            "average_speed": average_speed,
            "occupancy_rate": occupancy_rate,
            "event_time": datetime.now().isoformat()
        }
        return event

# =================MAIN=================
def main():
    print("Initialisation du Producer Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Connecté à Kafka sur {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"Erreur de connexion Kafka : {e}")
        return

    # Initialisation des capteurs (1 capteur par route pour simplifier, distribués dans les zones)
    sensors = []
    for road in ROADS:
        zone = random.choice(ZONES)
        sensor = TrafficSensor(road['id'], road['type'], zone)
        sensors.append(sensor)
        print(f"Capteur initialisé: {sensor.sensor_id} sur {road['id']} ({zone})")

    print(f"\nLancement de la simulation vers le topic '{KAFKA_TOPIC}' (CTRL+C pour arrêter)...")
    try:
        while True:
            for sensor in sensors:
                event = sensor.generate_event()
                # Envoi vers Kafka
                producer.send(KAFKA_TOPIC, event)
                # Affichage avec occupation
                print(
                    f"Envoyé: {event['sensor_id']} | "
                    f"Route: {event['road_id']} | "
                    f"Vitesse: {event['average_speed']} km/h | "
                    f"Occupation: {event['occupancy_rate']}%"
                )
            
            producer.flush()  # S'assurer que les messages partent
            time.sleep(2)     # Pause de 2 secondes entre chaque batch
    except KeyboardInterrupt:
        print("\nSimulation arrêtée.")
        producer.close()
    except Exception as e:
        print(f"Erreur pendant l'envoi : {e}")


if __name__ == "__main__":
    main()
