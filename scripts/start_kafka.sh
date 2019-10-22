#!/bin/bash

set -ex

DIR=/home/kafka
TIMEOUT=120

for i in 1 2 3; do
	KAFKA_PORT=$((i + 9090))

    KAFKA_HEAP_OPTS="-Xmx192m" $DIR/kafka-"$KAFKA_PORT"/bin/zookeeper-server-start.sh -daemon $DIR/kafka-"$KAFKA_PORT"/config/zookeeper.properties
done

for i in 1 2 3; do
	j=0
	while ! nc -z localhost $((2180+i)) && [ "$((j++))" -lt "$TIMEOUT" ]; do echo "Waiting %i"; sleep 1; done
	if [ "$j" -eq "$TIMEOUT" ]; then
		echo "Timeout waiting zookeeper($i) to start"
		cat /home/kafka/kafka-$((9090+i))/logs/zookeeper.out
		exit 1
	fi
done

sleep 1 # give time zookeeper to initialize

# Launch and wait for Kafka
for i in 1 2 3; do
	KAFKA_PORT=$((i + 9090))

    KAFKA_HEAP_OPTS="-Xmx320m" $DIR/kafka-"$KAFKA_PORT"/bin/kafka-server-start.sh -daemon $DIR/kafka-"$KAFKA_PORT"/config/server.properties
done

for i in 1 2 3; do
	j=0
	while ! nc -z localhost $((9090+i)) && [ "$((j++))" -lt "$TIMEOUT" ]; do echo "Waiting"; sleep 1; done
	if [ "$j" -eq "$TIMEOUT" ]; then
		echo "Timeout waiting Kafka ($i) to start"
		cat /home/kafka/kafka-$((9090+i))/logs/server.log
		exit 1
	fi
done

sleep 7 # give time kafka to initialize
