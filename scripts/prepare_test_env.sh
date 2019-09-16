#!/bin/bash

set -ex

sudo /usr/share/mysql-8.0/mysql-systemd-start pre
sudo /usr/bin/mysqld_safe --skip-syslog &
while ! /usr/bin/mysqladmin ping; do sleep 1; done
/etc/init.d/postgresql start
/etc/init.d/clickhouse-server start

sudo -H -u kafka /bin/sh scripts/start_kafka.sh

# Now startup Hadoop
service ssh start
sudo -u hadoop /bin/bash <<EOF
ssh-keyscan -H localhost > ~/.ssh/known_hosts
ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts
cd /home/hadoop
(sbin/start-dfs.sh) &
PID=\$!
wait \$PID
bin/hdfs dfs -mkdir -p /user/$USER
bin/hdfs dfs -chown -R $USER:$(id -g -n) /user/$USER
EOF


sudo -H -u hadoop /bin/bash <<EOF
cd /home/hadoop
rm -rf metastore_db
bin/hdfs dfs -mkdir -p /user/hive/warehouse
bin/hdfs dfs -chmod g+w /user/hive/warehouse
bin/hdfs dfs -mkdir -p /tmp
bin/hdfs dfs -chmod g+w /tmp
HADOOP_HOME=/home/hadoop bin/schematool -initSchema -dbType derby
EOF
