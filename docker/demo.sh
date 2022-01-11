#!/bin/bash

set -e

sql() {
    db=$1
    shift
    mysql -uroot -pstoragetapper -hdb "$db" -e "$@"
}


sql "" "DROP DATABASE IF EXISTS ex_db1"
sql "" "RESET MASTER"

sql "" "CREATE DATABASE ex_db1"
sql "ex_db1" "CREATE TABLE ex_table1(id int not null primary key, ts TIMESTAMP, type varchar(32))"

for i in `seq 1 10`; do
    sql "ex_db1" "INSERT INTO ex_table1(id, type) VALUES ($i, 'pre-existing')"
done


curl -s --data '{"cmd" : "add", "name" : "ex_cluster1", "host" : "db", "port" : 3306, "user" : "root", "pw" : "storagetapper"}' http://storagetapper:7836/cluster
curl -s --data '{"cmd" : "add", "cluster" : "ex_cluster1", "service" : "ex_svc1", "db":"ex_db1", "table":"ex_table1", "output":"kafka", "outputFormat":"json"}' http://storagetapper:7836/table


sleep 12

for j in `seq 1 100`; do

let start=$j*100
let end=$start+10

for i in `seq $start $end`; do
    sql "ex_db1" "INSERT INTO ex_table1(id, type) VALUES ($i, 'incremental')"
    sleep 1
done

sql "ex_db1" "ALTER TABLE ex_table1 ADD schema_change varchar(32)"

for i in `seq $start $end`; do
    sql "ex_db1" "UPDATE ex_table1 SET type = 'update' WHERE id = $i;"
    sleep 1
done

sql "ex_db1" "ALTER TABLE ex_table1 DROP schema_change"

done