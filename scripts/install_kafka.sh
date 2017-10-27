#!/bin/bash

set -ex

DIR=/home/kafka

sudo apt-get install default-jre
sudo useradd kafka -m || [ $? -eq 9 ]
sudo -u kafka mkdir -p $DIR/config
cd $DIR
#echo 'ruok' | telnet localhost 2181
[ -f kafka_2.11-0.8.2.1.tgz ] || sudo -u kafka wget "http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.1/kafka_2.11-0.8.2.1.tgz" -O kafka_2.11-0.8.2.1.tgz 
sudo -u kafka tar -xzf kafka_2.11-0.8.2.1.tgz --strip 1
sudo /bin/bash -c "echo -e \"delete.topic.enable = true\nnum.partitions=8\n\" >> $DIR/config/server.properties"

if [ -n "$DOCKER_BUILD" ]; then
	exit
fi

sudo -u kafka /bin/bash -c "nohup $DIR/bin/zookeeper-server-start.sh /home/kafka/config/zookeeper.properties >$DIR/zookeeper.log 2>&1 &"
sudo -u kafka /bin/bash -c "nohup $DIR/bin/kafka-server-start.sh /home/kafka/config/server.properties >$DIR/kafka.log 2>&1 &"
sleep 2

#echo "Hello, World" | ~/bin/kafka-console-producer.sh --broker-list localhost:9093 --topic TutorialTopic > /dev/null
#~/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TutorialTopic --from-beginning
