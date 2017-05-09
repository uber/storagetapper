Options
-------

  * **log_type** - Logger type
     Included logger plugins:
       * std -- standard Go [log](https://golang.org/pkg/log/) to stderr
       * zap -- [Uber zap](https://github.com/uber-go/zap) logger
       * logrus -- [Logrus](https://github.com/sirupsen/logrus) logger
  * **log_level** -- Log level. One of: fatal, error, warn, info, debug
  * **port** -- Port service binds it's https endpoints to
  * **max_num_procs** -- Number of workers started per instance
      This is not used if **reader_pipe_type** is **local**
  * **state_update_timeout** -- How often configuration in shared state will be loaded and updated (seconds)
  * **state_connect_url** -- This specifies state connection information. Format: user:password@host:port
  * **kafka_addresses** -- List of Kafka brokers addresses. Format: host:port
  * **reader_pipe_type** -- Specifies pipe type between storage reader and streamers. Currently supported:
      * **local** -- Golang channel based pipes. Streamers in the same process, reader controls number of streamers
      * **kafka** -- Messages between reader and streamers buffered in Kafka
  * **output_pipe_type** -- Default output pipe type. Currently supported:
      * **kafka** - Events destination is Kafka
  * **reader_output_format** - Reader produces messages in this format. Currently supported formats:
      * **json** -- Common JSON format described in [Common format](./commonformat.md) section
      * **avro** -- [Avro]() encoded events produced
  * **output_format** -- Output events format. Currently supported:
      * **json**
      * **avro**
  * **concurrent_bootstrap** -- Stream initial snapshot for the table concurrently with log events. It's a must with **local** reader pipe, but not enforced currently
  * **output_topic_name_format** - Allows to vary the output topic name format. Default: hp-%s-%s-%s (Placeholder are for: service name, database name, table name)
  * **buffer_topic_name_format** - Allow to vary intermediate buffer topic name format. Default: storagetapper.service.%s.db.%s.table.%s
  * **pipe_batch_size** - Maximum number of messages to push to the pipe at once
  * **output_pipe_concurrency** - Allow multiple streamers for the same table
  * **force_master_connection** - Snapshot is usually read from slave, this option allow to force all connections go to master server.

  * **throttle_target_mb** -- Throttle target bandwidth in megabytes
  * **throttle_target_iops** -- Throttle target IOPS

  * **logging** - Logger plugin specific options
