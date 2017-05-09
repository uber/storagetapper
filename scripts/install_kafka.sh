#!/bin/bash
set -ex
sudo apt-get update
sudo apt-get install default-jre zookeeper=3.4.5+dfsg-2+deb8u1 libzookeeper-java=3.4.5+dfsg-2+deb8u1 zookeeperd
sudo useradd kafka -m
sudo passwd kafka
sudo adduser kafka sudo
su - kafka
mkdir -p /home/kafka/config
cd /home/kafka
#echo 'ruok' | telnet localhost 2181
[ -f kafka_2.11-0.8.2.1.tgz ] || wget "http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.1/kafka_2.11-0.8.2.1.tgz" -O kafka_2.11-0.8.2.1.tgz 
tar -xvzf kafka_2.11-0.8.2.1.tgz --strip 1
echo "delete.topic.enable = true" >> /home/kafka/config/server.properties

nohup ~/bin/kafka-server-start.sh ~/config/server.properties > ~/kafka.log 2>&1 &
sleep 2

echo "Hello, World" | ~/bin/kafka-console-producer.sh --broker-list localhost:9093 --topic TutorialTopic > /dev/null
~/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TutorialTopic --from-beginning

