import json
import random
import time
import requests

import uuid
import datetime
from kafka import KafkaProducer

# faire : pip install requests


#start=2023-12-07&end=2023-12-09&token=X5kufRU4yq1dJ5WUmF0PvmYqKfuidLbSJ5rSPfapbqYbZKIn9ALXGw
# Configuration du producteur Kafka
producer = KafkaProducer (
    bootstrap_servers = ['proud-termite-7506-eu2-kafka.upstash.io:9092'],
    sasl_mechanism = 'SCRAM-SHA-256',
    security_protocol = 'SASL_SSL',
    sasl_plain_username = 'cHJvdWQtdGVybWl0ZS03NTA2JKhfRrReNmJX9Kw9RU42MUeAw-Rzzt7AtDLHquA',
    sasl_plain_password = 'ZmMzYjZjYmUtZjA0Mi00ZTlkLWE1MjktN2JkNWI0ZTBkYzVi',
)

# Fonction pour obtenir les données météorologiques depuis l'API infoclimat
def get_weather_data(city,start =datetime.datetime.now().strftime("%Y-%m-%d") ,end = datetime.datetime.now().strftime("%Y-%m-%d")):

    base_url = 'https://www.infoclimat.fr/opendata/?method=get&format=json&'
    if type(city) == type(list) :
        for elt in city:
            base_url += "stations[]="+elt+"&"
        params = {
            'start': start,
            'end': end, 
            'token' : 'X5kufRU4yq1dJ5WUmF0PvmYqKfuidLbSJ5rSPfapbqYbZKIn9ALXGw'
        }
    else :
        params = {
            'stations[]': city,
            'start': start,
            'end': end, 
            'token' : 'X5kufRU4yq1dJ5WUmF0PvmYqKfuidLbSJ5rSPfapbqYbZKIn9ALXGw'
        }
    response = requests.get(base_url, params=params)
    data = response.json()
    # print(data["hourly"]["00061"])
    # print(data["stations"])

    # Créer un dictionnaire de correspondance entre les IDs et les noms de station
    id_to_station = {station["id"]: station["name"] for station in data["stations"]}

    # Accéder à la liste "hourly" dans votre objet JSON
    hourly_list = data["hourly"]

    # Parcourir la liste "hourly" et mettre à jour les valeurs de "id_station" avec les noms correspondants
    for entry in hourly_list:
        station_id = entry
        for elt in hourly_list[entry] :
            # Vérifier si l'ID de la station existe dans le dictionnaire de correspondance
            if station_id in id_to_station:
                elt["id_station"] = id_to_station[station_id]
                elt["location"] = elt.pop("id_station")
                elt["temperature_ressentie"] = None
                elt["temperature_min"] = None
                elt["temperature_max"] = None
                elt["vent"] = elt.pop("vent_moyen")
                elt["qualite_air"] = random.uniform(0, 100)
                elt["timestamp"] = elt.pop("dh_utc")

    print(data["hourly"]["00061"][1])

    # for elt in data["hourly"]:
    #     meteo = {
    #         'location': city,
    #         'temperature': data['main']['temp'],
    #         'temperature_ressentie': data['main']['feels_like'],
    #         'temperature_min': data['main']['temp_min'],
    #         'temperature_max': data['main']['temp_max'],
    #         'pression': data['main']['pressure'],
    #         'humidite': data['main']['humidity'],
    #         'vent': data['wind']['speed'],
    #         'description': data['weather'][0]['description'],
    #         'qualite_air': random.uniform(0, 100),
    #         'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #     }
    # conversion des données sous format JSON.
    # log_entry = json.dumps(meteo)
    # return log_entry


get_weather_data(["00061","00081"])

    # print(meteo)


# print(int(datetime.datetime.now().strftime("%d"))-1)
# while True:

    # Envoyer les données au topic Kafka
    # producer.send('projet_1', key=str(uuid.uuid4()).encode('utf-8'), value=meteo_data.encode('utf-8'))
    # producer.flush()

    # time.sleep(5)  # Récupérez les données toutes les 5 secondes
