# build docker image:
# docker build . -t kafka-consumer-prediction

# run docker container:
# docker run --network="kafka_cluster_default" --volume $(pwd)/data:/data:ro -it kafka-consumer-prediction

FROM python:3.7

ADD ./requirements.txt ./
ADD ./consumer.py ./

RUN mkdir /data
RUN pip install -r requirements.txt

CMD ["python",  "consumer.py"]
