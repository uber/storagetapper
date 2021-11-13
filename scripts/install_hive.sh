#!/bin/bash

set -ex

NAME=hadoop
DIR=/home/$NAME
VERSION=2.3.9

sudo apt-get install --force-yes -y default-jre wget thrift-compiler
sudo useradd $NAME -m || [ $? -eq 9 ]
cd $DIR
[ -f apache-hive-${VERSION}-bin.tar.gz ] || sudo -H -u $NAME wget "http://apache.cs.utah.edu/hive/hive-${VERSION}/apache-hive-${VERSION}-bin.tar.gz" -O apache-hive-${VERSION}-bin.tar.gz
echo "0ad229e20e30d259d20ea6ede5c8fd4000c5844dac9e40e7f35128e0cdc55cc4f71046582db0a6c63effada4599952f8b4b10f2c394c042657aba134bff9c598 apache-hive-${VERSION}-bin.tar.gz" | sha512sum -c
sudo -H -u $NAME /bin/bash <<EOF
set -ex
tar -xzf apache-hive-${VERSION}-bin.tar.gz --strip 1
if [ -z "$DOCKER_BUILD" ]; then
	rm -rf metastore_db
	$DIR/bin/hdfs dfs -mkdir -p /user/hive/warehouse
	$DIR/bin/hdfs dfs -chmod g+w /user/hive/warehouse
	$DIR/bin/hdfs dfs -mkdir -p /tmp
	$DIR/bin/hdfs dfs -chmod g+w /tmp
	HADOOP_HOME=$DIR bin/schematool -initSchema -dbType derby
fi
EOF
