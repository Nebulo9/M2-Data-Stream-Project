import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, when
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
        StructField("location", StringType(), True),
        StructField("temperature", FloatType(), True),
        StructField("temperature_ressentie", FloatType(), True),
        StructField("temperature_min", FloatType(), True),
        StructField("temperature_max", FloatType(), True),
        StructField("pression", FloatType(), True),
        StructField("humidite", StringType(), True),
        StructField("vent", FloatType(), True),
        StructField("description", StringType(), True),
        StructField("qualite_air", FloatType(), True),
        StructField("timestamp", StringType(), True),
    ])

    stream_data = lines.select(
        F.from_json(lines.value, schema).alias("data")
    ).select("data.*")

    # Pluie :
    #     Risque de pluie faible : 1-30%
    #     Risque de pluie modéré : 31-60%
    #     Risque de pluie élevé : 61-100%

    result_pluie = stream_data.withColumn("risque_pluie", 
                                    when((col('humidite') >= 70) & (col('description').like('%cloud%')), "oui")
                                    .when((col('description').like('%rain%')), "oui")
                                    .otherwise("aucun risque"))

    result_pluie = result_pluie.select("location", "temperature", "risque_pluie")   



    # Chaleur élevée :
    #     Risque de chaleur modérée: 27-32°C
    #     Risque de chaleur élevée: 33-39°C
    #     Risque de chaleur extrême: 40°C et plus

    result_chaleur = stream_data.withColumn("risque_chaleur", 
                                    when((col('temperature') >= 27) & (col('temperature') < 32), "modérée")
                                    .when((col('temperature') >= 33) & (col('temperature') < 39), "élevée")
                                    .when((col('temperature') >= 40), "extrême")
                                    .otherwise("aucun risque"))

    result_chaleur = result_chaleur.select("location", "temperature", "risque_chaleur")

    # Gel :
    #     Risque de gel léger : Température entre 0°C et -2°C
    #     Risque de gel modéré : Température entre -2°C et -5°C
    #     Risque de gel sévère : Température en dessous de -5°C

    result_gel = stream_data.withColumn("risque_gel", 
                                    when((col('temperature') <= 0) & (col('temperature') > -2), "léger")
                                    .when((col('temperature') <= -2) & (col('temperature') > -5), "modéré")
                                    .when((col('temperature') <= -5), "sévère")
                                    .otherwise("aucun risque"))

    result_gel = result_gel.select("location", "temperature", "risque_gel")

    query = result_gel\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .queryName("result_table") \
        .trigger(processingTime="1 second")\
        .start()
    
    query.awaitTermination()

    # Rafales de vent :
    #     Rafales modérées : 30-50 km/h
    #     Rafales fortes : 51-80 km/h
    #     Rafales très fortes : 81 km/h et plus

    result_rafale = stream_data.withColumn("risque_rafale", 
                                    when((col('vent') >= 30) & (col('vent') < 50 ), "modérées")
                                    .when((col('vent') >= 50) & (col('vent') < 80), "fortes")
                                    .when((col('vent') >= 80), "très fortes")
                                    .otherwise("aucun risque"))

    result_rafale = result_rafale.select("location", "vent", "risque_rafale")

    # Humidité :
    #     Humidité relative élevée : Supérieure à 70%
    #     Humidité relative modérée : Entre 40% et 70%
    #     Humidité relative basse : Inférieure à 40%

    result_humidite = stream_data.withColumn("risque_humidite", 
                                    when((col('humidite') < 40), "basse")
                                    .when((col('humidite') >= 40) & (col('humidite') < 70), "modérée")
                                    .otherwise("élevée"))

    result_humidite = result_humidite.select("location", "humidite", "risque_humidite")

