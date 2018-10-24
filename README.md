## Spark Embedded Hive Thrift Server Example

A Simple Spark driver application with an embedded Hive Thrift server facilitating the querying of Parquet files as tables via a JDBC SQL endpoint. 

* Runs as a standalone local Spark application, as a driver application or submittable to a Spark cluster.  
* Exposes a JDBC SQL endpoint using the embedded Hive Thrift server.
* Registers Parquet files as tables to allow SQL querying by non Spark applications over JDBC.
* Simple REST interface for table registration, de-registration and listing of registered tables.

### Build
```
$ git clone https://github.com/nwrs/spark-sql-server.git
$ cd spark-sql-server
$ mvn clean package
```

### Command Line Options
```
$ java -jar spark-sql-server-1.0-SNAPSHOT-packaged.jar --help

Custom Spark Hive SQL Server [github.com/nwrs/spark-sql-server]

Usage:

  -n, --appName  <app_name>                        Spark application name.
  -j, --jdbcPort  <n>                              Hive JDBC endpoint port, defaults to 10000.
  -m, --master  <spark://host:port>                Spark master, defaults to 'local[*]'.
  -p, --restPort  <n>                              Rest port, defaults to 8181.
  -k, --sparkOpts  <opt=value,opt=value,...>       Additional Spark options for when running standalone.
  -t, --tableConfig  </path/to/TableConfig.conf>   Table configuration file.
  -h, --help                                       Show help message
```

### Running as Standalone Driver Application
```
$ java -jar spark-sql-server-1.0-SNAPSHOT-packaged.jar \
    --appName ParquetSQLServer \
    --master spark://spark-master-server:7077 \
    --jdbcPort 10001 \
    --restPort 8181 \
    --sparkOpts spark.executor.memory=4g, spark.default.parallism=50 \
    --tableConfig /Users/nwrs/parquet-tables.config
```  
## Example Table Config
```
$ cat example-tables.config

#Example table config file
tweets=hdfs://localhost:9000/tweets/tweets.parquet
users=hdfs://localhost:9000/users/users.parquet 

```
## Rest Interface

| VERB          | Path            |         Action             | Request Body | Response Body
| :------------- | :--------------- | :-------------------------- | :--- | :---
| GET           | /tables         | List all registered tables | None | JSON |
| GET           | /table/my_table | Get registration for my_table | None | JSON | 
| DELETE        | /table/my_table | De-register (drop) my_table| None | None |
| POST          | /table          | Register a table | JSON | None |

Example JSON to register a table via POST:
```
{
    "table": "my_table",
    "file": "hdfs://hadoop-server:9000/data/tweet/tweets.parquet"
}
```

## Connecting to the JDBC Endpoint
 
* The default JDBC endpoint connection string is: "jdbc:hive2://localhost:10000/default"
* Driver class name: org.apache.hive.jdbc.HiveDriver
* Dependencies available [here](Seehttp://www.mvnrepository.com/artifact/org.apache.hive/hive-jdbc)


See [here](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC) for more information on connecting to Hive via the JDBC driver.


