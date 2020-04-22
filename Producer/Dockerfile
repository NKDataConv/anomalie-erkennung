# build docker image:
# docker build . -t kafka-producer

# run docker container:
# docker run --network="kafka_cluster_default" -it kafka-producer

FROM python:3.7

ADD ./requirements.txt ./
ADD ./producer.py ./
ADD ./ec2_cpu_utilization_c6585a.csv ./
ADD ./ec2_disk_write_bytes_c0d644.csv ./
ADD ./ec2_network_in_257a54.csv ./

RUN pip install -r requirements.txt

CMD ["python",  "producer.py"]
