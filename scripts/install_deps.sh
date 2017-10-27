#!/bin/sh

set -ex

sudo apt-get -qq update
sudo apt-get install -y mysql-server-5.6
echo "[mysqld]\nserver-id=1\nbinlog-format=ROW\ngtid_mode=ON\nenforce-gtid-consistency\nlog_bin=/var/log/mysql/mysql-bin.log\nlog_slave_updates=1"|sudo tee -a /etc/mysql/my.cnf
sudo service mysql restart
go get github.com/Masterminds/glide
go get github.com/alecthomas/gometalinter
go get github.com/tinylib/msgp
gometalinter --install
/bin/sh scripts/install_kafka.sh
/bin/sh scripts/install_hadoop.sh
