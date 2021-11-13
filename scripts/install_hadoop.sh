#!/bin/bash

set -ex

NAME=hadoop
DIR=/home/$NAME
VERSION=3.3.1
SHA512="2fd0bf74852c797dc864f373ec82ffaa1e98706b309b30d1effa91ac399b477e1accc1ee74d4ccbb1db7da1c5c541b72e4a834f131a99f2814b030fbd043df66"

sudo useradd $NAME -m || [ $? -eq 9 ]
cd $DIR
[ -f hadoop-$VERSION.tar.gz ] || sudo -H -u $NAME wget "https://mirrors.ocf.berkeley.edu/apache/hadoop/common/hadoop-$VERSION/hadoop-$VERSION.tar.gz" -O hadoop-$VERSION.tar.gz
echo "$SHA512 hadoop-$VERSION.tar.gz" | sha512sum -c
sudo -H -u $NAME tar -xzf hadoop-$VERSION.tar.gz --strip 1

cat << 'EOF' | sudo -H -u $NAME tee $DIR/etc/hadoop/core-site.xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

cat << 'EOF' | sudo -H -u $NAME tee $DIR/etc/hadoop/hdfs-site.xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.datanode.handler.count</name>
        <value>20</value>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>1048576</value>
    </property>
</configuration>
EOF

sudo -H -u $NAME /bin/bash <<EOF
sed -i 's+# export JAVA_HOME=.*+export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f  /usr/bin/java)))+' etc/hadoop/hadoop-env.sh
if [ ! -f ~/.ssh/id_rsa ]; then
	ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
	cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys
	chmod 0600 ~/.ssh/authorized_keys
	ssh-keyscan -H localhost > ~/.ssh/known_hosts
	ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts
	rm -rf /tmp/hadoop-hadoop
fi
bin/hdfs namenode -format -force
if [ -z "$DOCKER_BUILD" ]; then
       (sbin/start-dfs.sh) &
       PID=\$!
       wait \$PID
       bin/hdfs dfs -mkdir -p /user/$USER
       bin/hdfs dfs -chown -R $USER:$(id -g -n) /user/$USER
fi
EOF
