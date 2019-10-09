#!/bin/sh

set -ex

/bin/sh scripts/install_go.sh
/bin/sh scripts/install_mysql.sh
/bin/sh scripts/install_kafka.sh
/bin/sh scripts/install_hadoop.sh
/bin/sh scripts/install_sql.sh
/bin/sh scripts/install_hive.sh
