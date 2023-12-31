import json
import random
import requests
import time
import uuid
import datetime
from kafka import KafkaProducer

# faire : pip install requests



# Configuration du producteur Kafka
producer = KafkaProducer (
    bootstrap_servers = ['proud-termite-7506-eu2-kafka.upstash.io:9092'],
    sasl_mechanism = 'SCRAM-SHA-256',
    security_protocol = 'SASL_SSL',
    sasl_plain_username = 'cHJvdWQtdGVybWl0ZS03NTA2JKhfRrReNmJX9Kw9RU42MUeAw-Rzzt7AtDLHquA',
    sasl_plain_password = 'ZmMzYjZjYmUtZjA0Mi00ZTlkLWE1MjktN2JkNWI0ZTBkYzVi',
)

# Fonction pour obtenir les données météorologiques depuis l'API OpenWeatherMap
def get_weather_data(city):
    base_url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {
        'q': city,
        'appid': '3aae7c70c4d11b7635aa3de50e87a514',
        'units': 'metric', 
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    meteo = {
        'location': city,
        'temperature': data['main']['temp'],
        'temperature_ressentie': data['main']['feels_like'],
        'temperature_min': data['main']['temp_min'],
        'temperature_max': data['main']['temp_max'],
        'pression': data['main']['pressure'],
        'humidite': data['main']['humidity'],
        'vent': data['wind']['speed'],
        'description': data['weather'][0]['description'],
        'qualite_air': random.uniform(0, 100),
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    print(meteo)

    # conversion des données sous format JSON.
    log_entry = json.dumps(meteo)
    return log_entry


while True:
    # Générer des données météorologiques réelles pour différentes villes
    cities = ['Paris', 'New York', 'Tokyo', 'Sydney', 'Rio de Janeiro']

    for city in cities:
        meteo_data = get_weather_data(city)

        # Envoyer les données au topic Kafka
        producer.send('projet_1', key=str(uuid.uuid4()).encode('utf-8'), value=meteo_data.encode('utf-8'))
        producer.flush()

    time.sleep(5)  # Récupérez les données toutes les 5 secondes
