#!/bin/bash
set -ex
sudo apt-get install default-jre zookeeperd
sudo useradd kafka -m
sudo -u kafka mkdir -p /home/kafka/config
cd /home/kafka
#echo 'ruok' | telnet localhost 2181
[ -f kafka_2.11-0.8.2.1.tgz ] || sudo -u kafka wget "http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.1/kafka_2.11-0.8.2.1.tgz" -O kafka_2.11-0.8.2.1.tgz 
sudo -u kafka tar -xvzf kafka_2.11-0.8.2.1.tgz --strip 1
sudo /bin/bash -c "echo -e \"delete.topic.enable = true\nnum.partitions=8\n\" >> /home/kafka/config/server.properties"

sudo -u kafka nohup ~/bin/kafka-server-start.sh ~/config/server.properties > ~/kafka.log 2>&1 &
sleep 2

#echo "Hello, World" | ~/bin/kafka-console-producer.sh --broker-list localhost:9093 --topic TutorialTopic > /dev/null
#~/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TutorialTopic --from-beginning
