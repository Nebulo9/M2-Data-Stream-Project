import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col, when
import os
from notif import send_notification


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

def send(row:dict[str,str]):
    lieu = row['loaction']
    risques = {
        "pluie":row['risque_pluie'],
        "chaleur":row['risque_chaleur'],
        "gel":row['risque_gel'],
        "rafale":row['risque_rafale'],
        "humidite":row['risque_humidite'],
    }
    for nature,risque in risques.items():
        if any(r in risque for r in ["aucun","bas"]):
            continue
        else:
            nat = nature.strip("risque_")
            send_notification(f"Risque de {nat} à {lieu}", f"Risque {risque} de {nat} à {lieu}",threaded=True)

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
        StructField("timestamp", TimestampType(), True),
    ])

    stream_data:DataFrame = lines.select(
        F.from_json(lines.value, schema).alias("data")
    ).select("data.*")
    
    
    # Chaleur élevée :
    #     Risque de chaleur modérée: 27-32°C
    #     Risque de chaleur élevée: 33-39°C
    #     Risque de chaleur extrême: 40°C et plus

    # Gel :
    #     Risque de gel léger : Température entre 0°C et -2°C
    #     Risque de gel modéré : Température entre -2°C et -5°C
    #     Risque de gel sévère : Température en dessous de -5°C

    # Rafales de vent :
    #     Rafales modérées : 30-50 km/h
    #     Rafales fortes : 51-80 km/h
    #     Rafales très fortes : 81 km/h et plus

    # Humidité :
    #     Humidité relative élevée : Supérieure à 70%
    #     Humidité relative modérée : Entre 40% et 70%
    #     Humidité relative basse : Inférieure à 40%


    result = stream_data\
                        .withColumn("risque_pluie", when((col('humidite') >= 70) & (col('description').like('%cloud%')), "probable")
                                    .when((col('description').like('%rain%')), "probable")
                                    .otherwise("aucun")) \
                        .withColumn("risque_chaleur", when((col('temperature') >= 27) & (col('temperature') < 32), "modéré")
                                    .when((col('temperature') >= 33) & (col('temperature') < 39), "élevé")
                                    .when((col('temperature') >= 40), "extrême") \
                                    .otherwise("aucun")) \
                        .withColumn("risque_gel", when((col('temperature') <= 0) & (col('temperature') > -2), "léger")
                                    .when((col('temperature') <= -2) & (col('temperature') > -5), "modéré")
                                    .when((col('temperature') <= -5), "sévère")
                                    .otherwise("aucun")) \
                        .withColumn("risque_rafale", when((col('vent') >= 30) & (col('vent') < 50 ), "modéré")
                                    .when((col('vent') >= 50) & (col('vent') < 80), "fort")
                                    .when((col('vent') >= 80), "très fort")
                                    .otherwise("aucun")) \
                        .withColumn("risque_humidite", when((col('humidite') < 40), "bas")
                                    .when((col('humidite') >= 40) & (col('humidite') < 70), "modéré")
                                    .otherwise("élevé")) \
                        .groupBy("location") \
                        .agg(
                            F.max("timestamp").alias("timestamp"),
                            F.max("risque_pluie").alias("risque_pluie"),
                            F.max("risque_chaleur").alias("risque_chaleur"),
                            F.max("risque_gel").alias("risque_gel"),
                            F.max("risque_rafale").alias("risque_rafale"),
                            F.max("risque_humidite").alias("risque_humidite")
                        )\

    query = result \
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .queryName("result_table") \
        .trigger(processingTime="1 second")\
        .foreach(lambda row: send(row.asDict()))\
        .start()
    
    query.awaitTermination()
