from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import OFFSET_BEGINNING

import pandas as pd
from joblib import dump
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt

def main():

    # create Avro Producer for Kafka
    # Note that only schema registry has to be given here and deserialization of avro is handled automatically
    c = AvroConsumer({
        'bootstrap.servers': 'broker:29092',
        'group.id': 'anomalie_training',
        'schema.registry.url': 'http://schema-registry:8081'})

    # subscribe to topic
    c.subscribe(['anomalie_tutorial'])

    # We need to change Kafka offset in order to consume from beginning.
    # There is no straightforward way to archive this, so first message had to
    # be polled in order that assignment can be obtained. Afterwards assignment
    # (in form of topic partition) is changed by setting the offset to the beginning.
    msg = c.poll(10)
    topic_partition = c.assignment()

    for partition in topic_partition:
        partition.offset = OFFSET_BEGINNING

    c.assign(topic_partition)

    # Consume messages frop topic
    messages = []
    while True:
        msg = c.poll(1)

        if msg is None:
            break

        messages.append(msg.value())

    c.close()

    # transform messages to Pandas DataFrame and feature engineering
    df = pd.DataFrame(messages)
    df.timestamp = pd.to_datetime(df.timestamp*1000000)

    df['hour'] = df.timestamp.dt.hour
    df['business_hour'] = ((df.hour < 8) | (df.hour>18)).astype("int")
    df.drop(["hour"], axis=1, inplace=True)

    # train test split
    # note that we can not use sklearn.model_selection.train_test_split as this is a time series and random split is not an option!
    train_length = int(len(df)*0.6)
    x_train = df.drop("timestamp", axis=1).iloc[:train_length,:]
    x_test = df.drop("timestamp", axis=1).iloc[train_length:,:]

    # Train Machine Learning Model, here Isolation Forests
    # contamination is import parameter. Determines how many datapoints will be classified as anomalous.
    iso_forest=IsolationForest(n_estimators=100, contamination=float(.02))
    iso_forest.fit(x_train)
    dump(iso_forest, '/data/iso_forest.joblib')

    # make predictions on test set
    predictions = iso_forest.predict(x_test)

    # make plot for evaluation and save figure
    evaluate_anomalies(predictions, df, train_length)

def evaluate_anomalies(predictions, df, train_length):
    # Create plot as save as png file in docker volume

    plt.subplots_adjust(hspace=0.4)
    ax1 = plt.subplot(311)
    plt.title("Network")
    plt.plot(df.network[train_length:], color='blue')
    plt.vlines(df.iloc[train_length:,:].index[predictions==-1], linewidth=1, ymin=0, ymax = max(df.network), color="red", linestyles="dashed")
    plt.grid(True)
    plt.setp(ax1.get_xticklabels(), visible=False)

    ax2 = plt.subplot(312, sharex=ax1)
    plt.title("CPU")
    plt.plot(df.cpu[train_length:], color='green')
    plt.vlines(df.iloc[train_length:,:].index[predictions==-1], linewidth=1, ymin=0, ymax = max(df.cpu), color="red", linestyles="dashed")
    plt.grid(True)
    plt.setp(ax2.get_xticklabels(), visible=False)

    ax3 = plt.subplot(313, sharex=ax1)
    plt.title("Disk")
    plt.plot(df.disk[train_length:], color='black')
    plt.vlines(df.iloc[train_length:,:].index[predictions==-1], linewidth=1, ymin=0, ymax = max(df.disk), color="red", linestyles="dashed")
    plt.grid(True)
    plt.xticks(rotation=45)

    plt.savefig("/data/evaluation_anomalien.png", quality=95)

if __name__ == '__main__':
    main()
