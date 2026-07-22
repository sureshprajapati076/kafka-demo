# How to run:

UPDATED NOTE WITH KAFKA IN DOCKER... (ZOOKEEPER LESS)

    Note: for Kafka version: 8.2.2 no zookeeper are needed, its zookeeper less architecture

    start: docker-compose up -d      >>> inside docker-compose.yml containing folder

    docker ps -> get name of kafka container: as docker-kafka-kafka-1

    docker exec -it docker-kafka-kafka-1 /bin/bash
    Create topic:
    kafka-topics --create --bootstrap-server localhost:9092 --topic terminal-topic --partitions 3 --replication-factor 1
    Consumer:
    kafka-console-consumer --bootstrap-server localhost:9092 --topic terminal-topic

    next terminal:
    docker exec -it docker-kafka-kafka-1 /bin/bash
    kafka-console-producer --bootstrap-server localhost:9092 --topic terminal-topic


# Outdated Notes:
make sure kafka in installed in C:\kafka_2.12-3.7.0 (as required version may change)

open zookeeper-startup.bat in intellij right click and run

open kafka-startup.bat in intellij right click and run
