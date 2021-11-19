Configuration
-------------

  * **changelog_buffer** -- Whether to use buffer between changelog reader and
  streamer to order snapshot and changelog streams (default: true)
  * **changelog_pipe_type** -- Specifies pipe type between storage reader and streamers. Currently supported:
      * **kafka** -- Messages between reader and streamers buffered in Kafka
      * **local** -- Golang channel based pipes. Streamers in the same process, reader controls number of streamers
  (default: kafka)
  * **changelog_topic_name_template_default** -- Template to configure changelog's output topic/file names based on task parameters.
  This is also the name of buffer topic if changelog buffering is used
  (default: "storagetapper.service.{{.Service}}.db.{{.DB}}.table.{{.Table}}{{if .Version}}.v{{.Version}}{{end}}")
  * **changelog_topic_name_template** -- Per source and destination changelog topic name configuration
  * **output_topic_name_template_default** -- Template to configure output topic/file names based on task parameters. Only snapshot produced to this topic if changelog buffering is not used
  * **output_topic_name_template** -- Per source and destination output topic name configuraion
  * **changelog_watchdog_interval** -- detect dead source (MySQL) servers (default: 300 seconds)
  * **config_refresh_interval** -- reload config from disk with this interval (default: 300 seconds)
  * **force_master_connection** -- Snapshot is usually read from slave, this option allow to force all connections go to master server.
  * **lock_expire_timeout** -- the task considered abandonded if lock update time is older then this timeout. Other worker is free to start working in this task (default: 2 * state_update_interval)
  * **log_level** -- Log level. One of: fatal, error, warn, info, debug
  * **log_type** -- Logger type (default: std)
     Included logger plugins:
       * std -- standard Go [log](https://golang.org/pkg/log/) to stderr
       * zap -- [Uber zap](https://github.com/uber-go/zap) logger
       * logrus -- [Logrus](https://github.com/sirupsen/logrus) logger
  (default: info)
  * **logging** - Logger plugin specific options
  * **max_num_procs** -- Number of workers started per instance (default: number of cpus)
  * **port** -- Port service binds it's http endpoints to (default: 7836)
  * **state_connect_url** -- This specifies state connection information. Format: user:password@host:port
  * **state_db_name** -- Allows to change state DB name (default: storagetapper)
  * **state_update_interval** -- How often configuration from shared state will be loaded and updated (default: 300 seconds)
  * **wokrer_idle_interval** -- Idle worker sleeps for this amount of time
  between checking for work in state (default: 30 seconds)

Following options can be configured globally as well as per task:

  * **cluster_concurrency** -- Run no more then this number of parallel
  snapshots from the cluster
  * **force_index** -- Force MySQL to use this index for snapshot
  * **no_snapshot** -- nNo snapshot required for the task
  * **throttle_target_mb** -- Throttle target bandwidth in megabytes
  * **throttle_target_iops** -- Throttle target IOPS
  * **no_delete_on_update** -- Produce only insert, instead of delete/insert pair on update
  * **row_filter** -- Provides conditions to filter rows when taking snapshot
  * **schedule** -- configre snapshot schedules
    * **interval** -- Enable periodic snapshots
  * **max_batch_size** - Maximum number of messages to push to the pipe at once
  * **max_batch_size_bytes** - Maximum number of bytes to push to the pipe at once
  * **base_dir** -- Base directory for file based pipe (default: /var/lib/storagetapper/)
  * **max_file_size** -- Maximum file size on disk (default: 1Gb)
  * **max_file_data_size** -- Maximum uncompressed data size in file
  * **compression** -- Compress file output
  * **file_delimited** -- Enables producing new-line delimited messages to text files and length prepended messages to binary files
  * **EndOfStreamMark** -- After producing last message of the stream write \_DONE file indicating that there will be no more files written to the directory
  * **encryption** -- Configure pipe encryption
    * **enabled** - Enable encryption
    * **public_key** -- Produce encrypts files with this key
    * **private_key** -- Consumer decrypts files with this key
    * **signing_key** -- Used to sign in producer and verify in consumer
  * **s3** -- Configure S3 pipe
    * **region**
    * **endpoint**
    * **bucket**
    * **base_dir**
    * **access_key_id**
    * **secret_access_key**
    * **session_token**
    * **timeout** (default: 7 days)
  * **kafka** -- Configure Kafka pipe
    * **addresses** -- Array of Kafka broker addresses in the form of "host:port"
    * **max_message_bytes** -- Maximum Kafka producer message size
  * **hadoop** -- Configure HDFS pipe
    * **user** -- User name to connecto to Hadoop
    * **addresses** -- Array of Hadoop hosts in the form of "host:port"
    * **base_dir** -- Base directory for output files
  * **sql** -- Configure SQL pipes
    * **type** -- Type of output on of: mysql, postgres, clickhouse
    * **dsn** -- Connection information in the form of corresponding Golang SQL driver
    * **service**, **cluster**, **db** -- User resolver to get connection information instead of DSN

Example config
--------------

```yaml
log_type: zap

logging:
  level: debug

state_update_interval: 10s
worker_idle_interval: 1s
state_connect_url: "storagetapper:storagetapper@localhost"

pipe:
    kafka:
        addresses:
            - "localhost:9091"
            - "localhost:9092"
            - "localhost:9093"
    hadoop:
        user: hadoop
        addresses:
            - "localhost:9000"
        base_dir: "/user/hadoop/tmp/hdfs_pipe_test"
    s3:
        region: "us-east-1"
        endpoint: "http://localhost:8000"
        access_key_id: 'accessKey1'
        secret_access_key: 'verySecretKey1'
        bucket: "pipetestbucket"
        base_dir: "/tmp/s3_pipe_test"

changelog_topic_name_template_default: "storagetapper-{{.Service}}-{{.DB}}-{{.Table}}{{if .Version}}-v{{.Version}}{{end}}"

output_topic_name_template:
  mysql:
    kafka:
        "hp-dbevents-mysql-{{.Service}}-{{.DB}}-{{.Table}}"
    hdfs: "{{.Input}}/{{.Service}}/{{.Table}}/{{.Version}}/{{.DB}}_"
    s3: "{{.Input}}/{{.Service}}/{{.Table}}/{{.Version}}/{{.DB}}_"
    mysql:
        "{{.DB}}.{{.Table}}"

max_file_size: 5368709120
```
