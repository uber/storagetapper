#!/bin/bash

set -ex

sudo /bin/bash -c "echo 'deb http://repo.yandex.ru/clickhouse/deb/stable/ main/'>/etc/apt/sources.list.d/clickhouse.list"
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-client clickhouse-server

#Avoid conflict with Hadoop default port
sudo sed -i 's+<tcp_port>9000</tcp_port>+<tcp_port>9500</tcp_port>+g' /etc/clickhouse-server/config.xml

sudo apt-get install --force-yes -y postgresql

#echo "host all postgres 127.0.0.1/32 trust" | sudo -H tee -a `find /etc/postgresql -name pg_hba.conf`
find /etc/postgresql -name pg_hba.conf -exec sudo -H sed -i -e 's/md5/trust/g' -e 's/peer/trust/g' {} \;

sudo /etc/init.d/postgresql restart
