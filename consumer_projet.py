import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col, when
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    #subscribeType should be "subscribe" in this exercise.
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
    
    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()
        
    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJvdWQtdGVybWl0ZS03NTA2JKhfRrReNmJX9Kw9RU42MUeAw-Rzzt7AtDLHquA\" password=\"ZmMzYjZjYmUtZjA0Mi00ZTlkLWE1MjktN2JkNWI0ZTBkYzVi\";")\
        .option("startingOffsets", "earliest")\
        .option(subscribeType, topics)\
        .option("auto.offset.reset", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    # Définir le schéma
    schema = StructType([
        StructField("coord", StructType([StructField("lon", FloatType(), True),StructField("lat", FloatType(), True)]), True),
        StructField("weather", StringType(), True),
        StructField("base", StringType(), True),
        StructField("main", StructType([StructField("temp", FloatType(), True),StructField("feels_like", FloatType(), True),StructField("temp_min", FloatType(), True),StructField("temp_max", FloatType(), True),StructField("pressure", IntegerType(), True),StructField("humidity", IntegerType(), True)]), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind", StructType([StructField("speed", FloatType(), True),StructField("deg", IntegerType(), True)]), True),
        StructField("clouds", StructType([StructField("all", IntegerType(), True)]), True),
        StructField("dt", IntegerType(), True),
        StructField("sys", StructType([StructField("type", IntegerType(), True),StructField("id", IntegerType(), True),StructField("country", StringType(), True),StructField("sunrise", IntegerType(), True),StructField("sunset", IntegerType(), True)]), True),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True)
    ])

    stream_data = lines.select(
        F.from_json(lines.value, schema).alias("data")
    ).select("data.*")

    exemple_data = {'coord': {'lon': 2.3488, 'lat': 48.8534}, 
        'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01n'}], 
        'base': 'stations', 
        'main': {'temp': 7.17, 'feels_like': 5.42, 'temp_min': 4.44, 'temp_max': 8.41, 'pressure': 1013, 'humidity': 93}, 
        'visibility': 10000, 
        'wind': {'speed': 2.57, 'deg': 240}, 
        'clouds': {'all': 0},
        'dt': 1702070718, 
        'sys': {'type': 2, 'id': 2041230, 'country': 'FR', 'sunrise': 1702020635, 'sunset': 1702050862}, 
        'timezone': 3600, 
        'id': 2988507, 
        'name': 'Paris', 
        'cod': 200
    }

    # Pluie :
    #     Risque de pluie faible : 1-30%
    #     Risque de pluie modéré : 31-60%
    #     Risque de pluie élevé : 61-100%

    result_pluie = stream_data.withColumn("risque_pluie", 
                                    when((col('main.humidity') >= 70) & (col('weather.description').like("%cloud%")), "oui")
                                    .when((col('weather.description').like("%rain%")), "oui")
                                    .otherwise("aucun risque"))

    result_pluie = result_pluie.select("name", "main.temp", "risque_pluie")

    # Chaleur élevée :
    #     Risque de chaleur modérée: 27-32°C
    #     Risque de chaleur élevée: 33-39°C
    #     Risque de chaleur extrême: 40°C et plus

    result_chaleur = stream_data.withColumn("risque_chaleur", 
                                    when((col('main.temp') >= 27) & (col('main.temp') < 32), "modérée")
                                    .when((col('main.temp') >= 33) & (col('main.temp') < 39), "élevée")
                                    .when((col('main.temp') >= 40), "extrême")
                                    .otherwise("aucun risque"))

    result_chaleur = result_chaleur.select("name", "main.temp", "risque_chaleur")

    # Gel :
    #     Risque de gel léger : Température entre 0°C et -2°C
    #     Risque de gel modéré : Température entre -2°C et -5°C
    #     Risque de gel sévère : Température en dessous de -5°C

    result_gel = stream_data.withColumn("risque_gel", 
                                    when((col('main.temp') <= 0) & (col('main.temp') > -2), "léger")
                                    .when((col('main.temp') <= -2) & (col('main.temp') > -5), "modéré")
                                    .when((col('main.temp') <= -5), "sévère")
                                    .otherwise("aucun risque"))

    result_gel = result_gel.select("name", "main.temp", "risque_gel")

    # Rafales de vent :
    #     Rafales modérées : 30-50 km/h
    #     Rafales fortes : 51-80 km/h
    #     Rafales très fortes : 81 km/h et plus

    result_rafale = stream_data.withColumn("risque_rafale", 
                                    when((col('wind.speed') >= 30) & (col('wind.speed') < 50 ), "modérées")
                                    .when((col('wind.speed') >= 50) & (col('wind.speed') < 80), "fortes")
                                    .when((col('wind.speed') >= 80), "très fortes")
                                    .otherwise("aucun risque"))

    result_rafale = result_rafale.select("name", "main.temp", "risque_rafale")

    # Humidité :
    #     Humidité relative élevée : Supérieure à 70%
    #     Humidité relative modérée : Entre 40% et 70%
    #     Humidité relative basse : Inférieure à 40%

    result_humidite = stream_data.withColumn("humidite", 
                                    when((col('main.humidity') < 40), "basse")
                                    .when((col('main.humidity') >= 40) & (col('main.humidity') < 70), "modérée")
                                    .otherwise("élevée"))

    result_humidite = result_humidite.select("name", "main.temp", "humidite")

