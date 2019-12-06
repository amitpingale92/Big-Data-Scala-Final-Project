#!/bin/sh
open -a Terminal.app zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

open -a Terminal.app kafka-server-start /usr/local/etc/kafka/server.properties

open -a Terminal.app kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Ford

open -a Terminal.app kafka-console-producer --broker-list localhost:9092 --topic Ford

open -a Terminal.app kafka-console-consumer --bootstrap-server localhost:9092 --topic Ford --from-beginning

# osascript -e kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic GM

# osascript -e kafka-console-producer --broker-list localhost:9092 --topic GM

# osascript -e kafka-console-consumer --bootstrap-server localhost:9092 --topic GM --from-beginning