import json
import random
import requests
import uuid
import datetime
from kafka import KafkaProducer

# Configuration du producteur Kafka
producer = KafkaProducer (
    bootstrap_servers = ['proud-termite-7506-eu2-kafka.upstash.io:9092'],
    sasl_mechanism = 'SCRAM-SHA-256',
    security_protocol = 'SASL_SSL',
    sasl_plain_username = 'cHJvdWQtdGVybWl0ZS03NTA2JKhfRrReNmJX9Kw9RU42MUeAw-Rzzt7AtDLHquA',
    sasl_plain_password = 'ZmMzYjZjYmUtZjA0Mi00ZTlkLWE1MjktN2JkNWI0ZTBkYzVi',
)

# Fonction pour obtenir les données météorologiques depuis l'API infoclimat
def get_weather_data(city,token,start =(datetime.datetime.now()- datetime.timedelta(hours=2)).strftime("%Y-%m-%d-%H") ,end = datetime.datetime.now().strftime("%Y-%m-%d")):

    base_url = 'https://www.infoclimat.fr/opendata/?method=get&format=json&'
    print(start)
    if type(city) == type(list) :
        for elt in city:
            base_url += "stations[]="+elt+"&"
        params = {
            'start': start,
            'end': end, 
            'token' : token
        }
    else :
        params = {
            'stations[]': city,
            'start': start,
            'end': end, 
            'token' : token
        }
    response = requests.get(base_url, params=params)
    data = response.json()
    # print(data["hourly"]["00061"])
    # print(data["stations"])

    # Créer un dictionnaire de correspondance entre les IDs et les noms de station
    id_to_station = {station["id"]: station["name"] for station in data["stations"]}

    # Accéder à la liste "hourly" dans votre objet JSON
    del data["hourly"]["_params"]
    hourly_list = data["hourly"]
    # Parcourir la liste "hourly" et mettre à jour les valeurs de "id_station" avec les noms correspondants
    for entry in hourly_list:
        station_id = entry
        for elt in hourly_list[entry] :
            # Vérifier si l'ID de la station existe dans le dictionnaire de correspondance
            if station_id in id_to_station:
                try :
                    elt["pluie_3h"] = float(elt["pluie_3h"])
                    elt["pluie_1h"] = float(elt["pluie_1h"])
                except TypeError :
                    elt["pluie_3h"] = 0
                    elt["pluie_1h"] = 0

                del elt["point_de_rosee"]
                del elt["vent_direction"]
                elt["id_station"] = id_to_station[station_id]
                elt["location"] = elt.pop("id_station")
                elt["temperature"] = float(elt["temperature"])
                elt["pression"] = float(elt["pression"])
                elt["temperature_min"] = None
                elt["temperature_max"] = None
                elt["vent"] = float(elt.pop("vent_moyen"))
                elt["qualite_air"] = random.uniform(0, 100)
                elt["timestamp"] = elt.pop("dh_utc")
                if float(elt["temperature"]) < 10 and  float(elt["vent"]) > 4.8:
                    elt["temperature_ressentie"] =f"{round(13.12 + 0.6215*float(elt['temperature']) + (0.3965 * float(elt['temperature']) -11.37) * float(elt['vent'])**0.16,1)}"
                elif float(elt["temperature"]) < 10 and  float(elt["vent"]) < 4.8:
                    elt["temperature_ressentie"] = f"{round(float(elt['temperature']) + (0.1345 * float(elt['temperature']) -1.59) * float(elt['vent']),1)}"
                else :
                    elt["temperature_ressentie"] = elt["temperature"]
    return data["hourly"]


TOKEN = "Eu7Wo7tufWLsVVsSWVZmGXTNw7jFhTNUTfiaYs4JCWUYrCL5THNxw"
data = get_weather_data(["00061","00081"],TOKEN)

# print(data)
for elt in data :
    for msg in data[elt] :
        # Envoyer les données au topic Kafka
        print(msg)
        producer.send('projet_1', key=str(uuid.uuid4()).encode('utf-8'), value=json.dumps(msg).encode('utf-8'))
        producer.flush()

print(data["00061"][0])
print(len(data["00061"][0]))
