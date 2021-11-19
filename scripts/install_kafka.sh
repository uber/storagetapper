#!/bin/bash

set -ex

DIR=/home/kafka
VERSION="2.8.1"
SCALA_VERSION="2.13"

sudo apt-get install default-jre netcat
sudo useradd kafka -m || [ $? -eq 9 ]
sudo -u kafka mkdir -p $DIR/config

mkdir -p $DIR

(
cd $DIR

[ -f kafka_$SCALA_VERSION-$VERSION.tgz ] || sudo -u kafka wget "https://archive.apache.org/dist/kafka/$VERSION/kafka_$SCALA_VERSION-$VERSION.tgz" -O kafka_$SCALA_VERSION-$VERSION.tgz && \
	echo "91FCD1061247AD0DDB63FA2B5C0251EE0E58E60CC9E1A3EBE2E84E9A31872448A36622DD15868DE2C6D3F7E26020A8C61477BC764E2FB6776A25E4344EB8892D kafka_$SCALA_VERSION-$VERSION.tgz" |sha512sum -c

for i in 1 2 3; do
	ZK_PORT=$((i + 2180))
	KAFKA_PORT=$((i + 9090))
	KAFKA_DATADIR="${DIR}/kafka-${KAFKA_PORT}/data"
	ZK_DATADIR="$DIR/zookeeper-$ZK_PORT"

	sudo -H -u kafka /bin/bash <<-EOF
	set -ex

	mkdir -p $DIR/kafka-$KAFKA_PORT
	tar xzf $DIR/kafka_$SCALA_VERSION-$VERSION.tgz -C $DIR/kafka-$KAFKA_PORT --strip-components 1

	sed -i -e "s/^broker.id=.*/broker.id=$i/g" -e "s/^zookeeper.connect=.*/zookeeper.connect=localhost:2181,localhost:2182,localhost:2183/g" $DIR/kafka-$KAFKA_PORT/config/server.properties
	echo -e "\\nport=$KAFKA_PORT\\ndelete.topic.enable = true\\nnum.partitions=8\\n" >> $DIR/kafka-$KAFKA_PORT/config/server.properties

	mkdir -p $KAFKA_DATADIR
	sed -i -e "s#log.dirs=.*#log.dirs=$KAFKA_DATADIR#g" $DIR/kafka-$KAFKA_PORT/config/server.properties

	sed -i -e "s#dataDir=.*#dataDir=$ZK_DATADIR#g" -e "s/clientPort=.*/clientPort=$ZK_PORT/g" $DIR/kafka-$KAFKA_PORT/config/zookeeper.properties
	echo -e "\\ninitLimit=10\\nsyncLimit=5\\nserver.1=localhost:2888:3888\\nserver.2=localhost:2889:3889\\nserver.3=localhost:2890:3890\\n" >>  $DIR/kafka-$KAFKA_PORT/config/zookeeper.properties

	mkdir -p $ZK_DATADIR

	echo $i > $DIR/zookeeper-$ZK_PORT/myid
	EOF
done
)

if [ -z "$DOCKER_BUILD" ]; then
	sudo -H -u kafka /bin/bash scripts/start_kafka.sh
fi
