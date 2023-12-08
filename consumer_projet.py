import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import from_json, col
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
    
    schema = T.StructType([
        T.StructField('location', T.StringType(), True),
        T.StructField('temperature', T.DoubleType(), True),
        T.StructField('temperature_ressentie', T.DoubleType(), True),
        T.StructField('humidite', T.DoubleType(), True),
        T.StructField('vent', T.DoubleType(), True),
        T.StructField('qualite_air', T.DoubleType(), True),
    ])

    stream_data = lines.select(
        F.from_json(lines.value, schema).alias("data")
    ).select("data.*")

    # Seuils pour la détection des risques météorologiques (ajustez selon vos besoins)
    seuil_risque_pluie = 70  # Exemple : humidité supérieure à 70%
    seuil_risque_neige = 0  # Exemple : température inférieure à 0 degré Celsius
    seuil_risque_canicule = 35  # Exemple : température supérieure à 35 degrés Celsius


    # Filtrage des données dépassant le seuil
    filtered_data = stream_data.filter(
        (col('humidite') > seuil_risque_pluie) |
        (col('temperature') < seuil_risque_neige) |
        (col('temperature') > seuil_risque_canicule)
    )

    # Affichage des données filtrées en temps réel
    query = filtered_data.writeStream \
        .outputMode('append') \
        .format('console') \
        .start()

    # Attente de la terminaison du stream
    query.awaitTermination()