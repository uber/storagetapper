StorageTapper
-------------

Overview
--------

[![Build Status](https://github.com/uber/storagetapper/workflows/Go/badge.svg)]()
[![Go Report Card](https://goreportcard.com/badge/github.com/uber/storagetapper)](https://goreportcard.com/report/github.com/uber/storagetapper)
[![codecov](https://codecov.io/gh/uber/storagetapper/branch/master/graph/badge.svg)](https://codecov.io/gh/uber/storagetapper)

StorageTapper is a scalable realtime MySQL change data streaming, logical backup
and logical replication service.

Storagetapper is deployed in production at Uber and used to produce snapshot and
realtime changed data of thousands of MySQL tables across multiple datacenters.

It is also used as a backup service to snapshot hundreds of terrabytes
of Schemaless data to HDFS and S3 with optional assymetric encryption and
compression.

It reads data from source transforms according to the specified event
format and produces data to destination.

Supported event sources:
* MySQL
* Schemaless

Supported event destinations:
* Kafka
* HDFS
* S3
* Local file
* MySQL (experimental)
* Postgres (experimental)
* Clickhouse (experimental)

Supported event formats:
* Avro
* [JSON](./doc/commonformat.md)
* MsgPack
* SQL

Storagetapper keeps it jobs state in MySQL database and automatically distribute jobs
between configured number of workers.

It is also aware of node roles and takes snapshot from the slave nodes in order 
to reduce load on master nodes. It can also optionally further throttles the reads.
Binlogs are streamed from master nodes for better SLAs.

Service is dynamically configurable through [RESTful API](./doc/endpoints.md) or
builtin UI.

Build & Install
---------------

## Debian & Ubuntu
```sh
cd storagetapper
make deb && dpkg -i ../storagetapper_1.0_amd64.deb
```

## Others
```sh
cd storagetapper
make && make install
```

## Development

### Linux

```sh
/bin/bash scripts/install_deps.sh # install all dependencies: MySQL, Kafka, HDFS, S3, ...
make test # run all tests
GO111MODULE=on TEST_PARAM="-test.run=TestLocalBasic" /bin/bash scripts/run_tests.sh ./pipe # individual test
```

### Non Linux
```sh
make test-env
$ make test
```

Configuration
-------------

Storagetapper loads configuration from the following files and location in the
given order:
```sh
    /etc/storagetapper/base.yaml
    /etc/storagetapper/production.yaml
    $(HOME)/base.yaml
    $(HOME)/production.yaml
    $(STORAGETAPPER_CONFIG_DIR)/base.yaml
    $(STORAGETAPPER_CONFIG_DIR)/production.yaml
```

Available options described in [Options](./doc/options.md) section

License
-------
This software is licensed under the [MIT License](LICENSE).

