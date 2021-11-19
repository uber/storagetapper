HTTP endpoints
--------------

## Builtin database resolver

This provides source MySQL cluster connection information mapping to unique
cluster name.

http://localhost:7836/cluster

```json
{"cmd" : "add", "name" : "cluster1", "host" : "localhost", "port" : 3306, "user" : "root", "pw" : ""},
{"cmd" : "del", "name" : "cluster1"}
```

## Control tables for ingestion

This endpoint controls table ingestion tasks.
Note: "service" field is deprecated. It was used as top level scope allowing
non-unique cluster names.

"http://localhost:7836/table"

```json
{"cmd" : "add", "cluster" : "cluster1", "service" : "service1", "db":"database1", "table":"table1"},
{"cmd" : "del", "cluster" : "cluster1", "service" : "service1", "db":"database1", "table":"table1"},
{"cmd" : "list", "cluster" : "cluster1", "service" : "service1", "db":"database1", "table":"table1"}
```
There is a helper CLI script, which provides convenient way to control table
onboarding and offboarding [stcli](../scripts/stcli) 

## Output schema store

Schema is only required for Avro format, for JSON and MsgPack it's still can be
added for field filtering for example. Fields which are ommitted in the schema,
won't be produce to destination.

http://localhost:7836/schema

```json
{"cmd" : "add", "name" : "name1", "schema": "schema body in Avro or JSON format"}
{"cmd" : "del", "name" : "name1"}
{"cmd" : "register", "service" : "service1", "db":"database1", "table":"table1"}
```
