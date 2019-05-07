StorageTapper
--------------

StorageTapper is a scalable realtime MySQL change data streaming and
transformation service.

Service reads data from MySQL, transforms it into an Avro schema
serialized format, and publishes these events to Kafka. Consumers can then
consume these events directly from Kafka.

[![Build Status](https://travis-ci.com/uber/storagetapper.svg?branch=master)](https://travis-ci.com/uber/storagetapper)
[![Go Report Card](https://goreportcard.com/badge/github.com/uber/storagetapper)](https://goreportcard.com/report/github.com/uber/storagetapper)
[![codecov](https://codecov.io/gh/uber/storagetapper/branch/master/graph/badge.svg)](https://codecov.io/gh/uber/storagetapper)

- [Features](#features)
- [Limitations](#limitations)
- [Build & Install](#build--install)
- [Configuration](#configuration)

Features
--------

  * Producing events to Kafka
  * Automatic work distribution between instances
  * Avro output format
  * [JSON](./doc/commonformat.md) output format
  * HTTP [endpoints](./doc/endpoints.md) to control
      * Tables to be ingested
      * Output schema
      * Database address resolver
  * Snapshot is taken from slave to reduce load on master
  * Binlogs streaming from master for better SLA
  * Throttling when taking snapshot

Limitations
-----------
  * Tables must have a primary key

Build & Install
---------------

## Get build dependencies
```sh
go get github.com/Masterminds/glide
go get github.com/alecthomas/gometalinter
```

It's recommended to have local MySQL and Kafka installed, so as many tests
depend on them. Tweak [development config](./config/development.yaml) to
correspond your setup.
**WARNING**: Tests run dangerous commands like RESET MASTER and DROP
DATABASE, so for tests don't use any MySQL instances with precious data.

## Debian & Ubuntu
```sh
cd storagetapper
make deb && dpkg -i ../storagetapper_1.0_amd64.deb
```

## Others
```sh
cd storagetapper
make test && make install
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

