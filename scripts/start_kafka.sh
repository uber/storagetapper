#!/bin/bash

set -ex

DIR=/home/kafka

for i in 1 2 3; do
    KAFKA_PORT=`expr $i + 9090`

    KAFKA_HEAP_OPTS="-Xmx192m" $DIR/kafka-$KAFKA_PORT/bin/zookeeper-server-start.sh -daemon $DIR/kafka-$KAFKA_PORT/config/zookeeper.properties
done
for i in 1 2 3; do
	while ! nc -q 1 localhost $((2180+i)) </dev/null; do echo "Waiting %i"; sleep 1; done
done

sleep 1 # give time zookeeper to initialize

# Launch and wait for Kafka
for i in 1 2 3; do
    KAFKA_PORT=`expr $i + 9090`

    KAFKA_HEAP_OPTS="-Xmx320m" $DIR/kafka-$KAFKA_PORT/bin/kafka-server-start.sh -daemon $DIR/kafka-$KAFKA_PORT/config/server.properties
done

for i in 1 2 3; do
	while ! nc -q 1 localhost $((9090+i)) </dev/null; do echo "Waiting"; sleep 1; done
done

sleep 7 # give time kafka to initialize
