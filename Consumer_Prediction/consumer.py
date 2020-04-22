from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from joblib import load
import pandas as pd
from datetime import datetime

if __name__ == '__main__':

    # Load ML Model from Docker Volume
    iso_forest = load('/data/iso_forest.joblib')

    # Start Avro Consumer. We do not need to provide the schema, this automatically taken care with the schema regsitry.
    c = AvroConsumer({
        'bootstrap.servers': 'broker:29092',
        'group.id': 'anomalie_prediction',
        'schema.registry.url': 'http://schema-registry:8081'})

    c.subscribe(['anomalie_tutorial'])

    # start consuming messages
    while True:
        try:
            msg = c.poll(1)

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            break

        if msg is None:
            continue

        if msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            continue

        # transform messages to format for prediction
        df = pd.DataFrame(msg.value(), index=[0])
        df.timestamp = pd.to_datetime(df.timestamp*1000000)

        df['hour'] = df.timestamp.dt.hour
        df['business_hour'] = ((df.hour < 8) | (df.hour>18)).astype("int")
        df_predict = df.drop(["hour", "timestamp"], axis=1)

        # predict anomalies
        anomalie = iso_forest.predict(df_predict)

        # -1 corresponds to anomaly. Every anomaly is just printed and shown in docker logs.
        anomalie_timestamps = df[anomalie==-1].timestamp
        for anomalie in anomalie_timestamps:
            print(f'Anomalie zum Zeitpunkt {anomalie}')

        else:
            print("Keine Anomalien.")

    c.close()
