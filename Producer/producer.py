from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from datetime import datetime
import pandas as pd
import json
import time

class MyProducer:
    '''Avro producer for Kafka'''

    def __init__(self):

        # This is the Avro Schema for messages
        self.value_schema_str = """
        {  "name": "value",
           "type": "record",
           "fields" : [
             {"name" : "network", "type" : "float"},
             {"name" : "disk", "type" : "float"},
             {"name" : "cpu", "type" : "float"},
             {"name" : "timestamp", "type" : "long"}
           ]
        }"""
        self.value_schema = avro.loads(self.value_schema_str)

        self.avroProducer = AvroProducer({
            'bootstrap.servers': 'broker:29092',
            'on_delivery': self.delivery_report,
            'schema.registry.url': 'http://schema-registry:8081'
            }, default_value_schema=self.value_schema)

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush().
            Source: https://github.com/confluentinc/confluent-kafka-python
        """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def main():

    #read data
    df_network = pd.read_csv("ec2_network_in_257a54.csv", parse_dates=["timestamp"], index_col="timestamp")
    df_disk = pd.read_csv("ec2_disk_write_bytes_c0d644.csv", parse_dates=["timestamp"], index_col="timestamp")
    df_cpu = pd.read_csv("ec2_cpu_utilization_c6585a.csv", parse_dates=["timestamp"], index_col="timestamp")

    # concatenate data
    df_concat = pd.concat([df_network, df_disk, df_cpu], axis=1)
    df_concat.columns= ["network", "disk", "cpu"]

    # timeseries overlap from 2014-04-10 till 2014-4-16
    mask_earliest = df_concat.index > pd.Timestamp('2014-04-10 00:04:00')
    mask_latest = df_concat.index < pd.Timestamp('2014-04-16 14:20:00')
    df_concat = df_concat[mask_earliest & mask_latest]

    # disk is measured at multiples of 5 minutes, whereas cpu and network is measured at minutes 04, 09, 14 etc.
    # therefore we do a foreward fill with disk and delete nans
    df_concat["disk"] = df_concat.disk.ffill()
    df_concat = df_concat.dropna(axis=0)

    # reset index as we want to include the timestamp in json
    df_concat.reset_index(inplace=True)

    # convert to json
    json_records = json.loads(df_concat.to_json(orient="records"))

    # Producer
    producer = MyProducer().avroProducer

    # publish to kafka topic
    # as this is simulation, between pushes is a break of 1 second (instead of 5 minutes, as is the case in the data)
    for record in json_records:
        producer.produce(topic='anomalie_tutorial', value=record)
        producer.flush()
        time.sleep(1)


if __name__ == '__main__':
    main()
