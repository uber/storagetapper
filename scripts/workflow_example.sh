#!/bin/sh

set -ex

go build

sql() {
	db=$1
	shift
	mysql -uroot "$db" -e "$@"
}

KAFKA_RETENTION_TIME=20

TEST_TOPIC=hp-ex_svc1-ex_db1-ex_table1
KPATH=/home/kafka/bin
KPARAM="--zookeeper localhost --topic $TEST_TOPIC"

$KPATH/kafka-topics.sh "$KPARAM" --alter --config retention.ms=$((KAFKA_RETENTION_TIME * 1000))
$KPATH/kafka-topics.sh "$KPARAM" --describe

sql "" "DROP DATABASE IF EXISTS ex_db1"
sql "" "RESET MASTER"

sql "" "CREATE DATABASE ex_db1"
sql "ex_db1" "CREATE TABLE ex_table1(f1 int not null primary key, ts TIMESTAMP, f3 int not null default 0)"

for i in $(seq 101 110); do
	sql "ex_db1" "INSERT INTO ex_table1(f1) VALUES ($i)"
done

./storagetapper &
TPID=$!
trap 'kill $TPID; exit 1' 1 2 15 #SIGHUP SIGINT SIGTERM
sleep 2

curl --data '{"cmd" : "add", "name" : "ex_cluster1", "host" : "localhost", "port" : 3306, "user" : "root", "pw" : ""}' http://localhost:7836/cluster
curl --data '{"cmd" : "add", "cluster" : "ex_cluster1", "service" : "ex_svc1", "db":"ex_db1", "table":"ex_table1"}' http://localhost:7836/table

sleep 12

for i in $(seq 1 10); do
	sql "ex_db1" "INSERT INTO ex_table1(f1) VALUES ($i)"
done

sql "ex_db1" "ALTER TABLE ex_table1 ADD f2 varchar(32)"

for i in $(seq 11 30); do
	sql "ex_db1" "INSERT INTO ex_table1(f1,f2) VALUES ($i, CONCAT('bbb', $i))"
done

sql "ex_db1" "ALTER TABLE ex_table1 DROP f2"

for i in $(seq 101 110); do
	sql "ex_db1" "UPDATE ex_table1 SET f3=f3+20 WHERE f1>100 AND f1<111"
done

sleep 4

curl --data '{"cmd" : "del", "name" : "ex_cluster1" }' http://localhost:7836/cluster
curl --data '{"cmd" : "del", "cluster" : "ex_cluster1", "service" : "ex_svc1", "db":"ex_db1", "table":"ex_table1"}' http://localhost:7836/table

kill $TPID

$KPATH/kafka-topics.sh "$KPARAM" --describe
$KPATH/kafka-topics.sh --list --zookeeper localhost:2181
$KPATH/kafka-console-consumer.sh "$KPARAM" --max-messages 50 --from-beginning

date
echo "Wait for $KAFKA_RETENTION_TIME secs before running next test"
