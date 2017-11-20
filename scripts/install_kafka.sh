#!/bin/bash

set -ex

DIR=/home/kafka

sudo apt-get install default-jre
sudo useradd kafka -m || [ $? -eq 9 ]
sudo -u kafka mkdir -p $DIR/config
cd $DIR
#echo 'ruok' | telnet localhost 2181
[ -f kafka_2.11-0.11.0.2.tgz ] || sudo -u kafka wget "http://mirror.metrocast.net/apache/kafka/0.11.0.2/kafka_2.11-0.11.0.2.tgz" -O kafka_2.11-0.11.0.2.tgz && \
	echo "0169DED7E551476FF3F1CE70D76C518D31116ACC1AE43B15E89BEA4B072FA8727239E51E84C2306DFD668EFFFC26DBC83C9598ACBD355C283F650C64C4788188 kafka_2.11-0.11.0.2.tgz" |sha512sum -c
sudo -u kafka tar -xzf kafka_2.11-0.11.0.2.tgz --strip 1
sudo /bin/bash -c "echo -e \"\ndelete.topic.enable = true\nnum.partitions=8\n\" >> $DIR/config/server.properties"


if [ -n "$DOCKER_BUILD" ]; then
	exit
fi

sudo -u kafka /bin/bash -c "nohup $DIR/bin/zookeeper-server-start.sh /home/kafka/config/zookeeper.properties >$DIR/zookeeper.log 2>&1 &"
sudo -u kafka /bin/bash -c "nohup $DIR/bin/kafka-server-start.sh /home/kafka/config/server.properties >$DIR/kafka.log 2>&1 &"
sleep 2

#echo "Hello, World" | ~/bin/kafka-console-producer.sh --broker-list localhost:9093 --topic TutorialTopic > /dev/null
#~/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TutorialTopic --from-beginning
