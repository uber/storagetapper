HTTP endpoints
--------------

## Builtin service database resolver

http://localhost:7836/cluster

```json
{"cmd" : "add", "name" : "cluster1", "host" : "localhost", "port" : 3306, "user" : "root", "pw" : ""},
{"cmd" : "del", "name" : "cluster1"}
```

## Control tables for ingestion

"http://localhost:7836/table"

```json
{"cmd" : "add", "cluster" : "cluster1", "service" : "service1", "db":"database1", "table":"table1"},
{"cmd" : "del", "cluster" : "cluster1", "service" : "service1", "db":"database1", "table":"table1"},
{"cmd" : "list", "cluster" : "cluster1", "service" : "service1", "db":"database1", "table":"table1"}
```

## Output schema store

http://localhost:7836/schema

```json
{"cmd" : "add", "name" : "name1", "schema": "schema body in Avro or JSON format"}
{"cmd" : "del", "name" : "name1"}`
{"cmd" : "register", "service" : "service1", "db":"database1", "table":"table1"}
```
