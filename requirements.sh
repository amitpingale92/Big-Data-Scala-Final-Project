#!/bin/sh
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

kafka-server-start /usr/local/etc/kafka/server.properties

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Ford

kafka-console-producer --broker-list localhost:9092 --topic Ford

kafka-console-consumer --bootstrap-server localhost:9092 --topic Ford --from-beginning

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic GM

kafka-console-producer --broker-list localhost:9092 --topic GM

kafka-console-consumer --bootstrap-server localhost:9092 --topic GM --from-beginning