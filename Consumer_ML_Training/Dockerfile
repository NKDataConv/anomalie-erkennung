# build docker image:
# docker build . -t kafka-consumer-training

# run docker container:
# docker run --network="kafka_cluster_default" --volume $(pwd)/data:/data:rw -it kafka-consumer-training

FROM python:3.7

ADD ./requirements.txt ./
ADD ./consumer.py ./

RUN pip install -r requirements.txt
RUN mkdir /data

CMD ["python",  "consumer.py"]
