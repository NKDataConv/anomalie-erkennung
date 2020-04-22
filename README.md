## Anomalie-Erkennung in Echtzeit mit Kafka und Isolation Forests

Dies ist das Repository für den Artikel.

**Wichtige Kommandos:**


Git Repo clonen und Kafka Cluster starten
```
git clone …
cd docker
docker-compose -f ./kafka_cluster/docker-compose.yml up -d --build
```

Ein Test Topic anlegen, Nachrichten verschicken und empfangen
```
docker-compose -f kafka_cluster/docker-compose.yml exec broker kafka-topics --create --bootstrap-server localhost:9092 --topic test-stream
docker-compose -f kafka_cluster/docker-compose.yml exec broker kafka-console-producer --broker-list localhost:9092 --topic test-stream
docker-compose -f kafka_cluster/docker-compose.yml exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic test-stream --from-beginning

```

Kafka Producer mit Testdaten starten
```
docker build ./producer -t kafka-producer
docker run --network="kafka_cluster_default" -it kafka-producer

```

Kafka Consumer für Training der Anomalie-Erkennung
```
docker build ./Consumer_ML_Training -t kafka-consumer-training
docker run --network="kafka_cluster_default" --volume $(pwd)/data:/data:rw -it kafka-consumer-training
```

Kafka Consumer für Auswertung der Anomalie-Erkennung
```
docker build ./Consumer_Prediction -t kafka-consumer-prediction
docker run --network="kafka_cluster_default" --volume $(pwd)/data:/data:ro -it kafka-consumer-prediction

```

Skalieren der Anomalie-Erkennung
```
docker-compose -f kafka_cluster/docker-compose.yml exec broker kafka-topics --alter --zookeeper zookeeper:2181 --topic anomalie_tutorial --partitions 2
docker run --network="kafka_cluster_default" --volume $(pwd)/data:/data:ro -it kafka-consumer-prediction
```
