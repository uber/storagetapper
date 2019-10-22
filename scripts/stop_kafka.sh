#!/bin/bash

set -x

DIR=/home/kafka

for i in 1 2 3; do
	KAFKA_PORT=$((i + 9090))
    $DIR/kafka-"$KAFKA_PORT"/bin/kafka-server-stop.sh
done

for i in 1 2 3; do
	KAFKA_PORT=$((i + 9090))
    $DIR/kafka-$KAFKA_PORT/bin/zookeeper-server-stop.sh
done
