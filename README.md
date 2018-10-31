## Spark SQL Server

A Spark driver application with an embedded Hive Thrift server facilitating the querying of Parquet files as tables via a JDBC SQL endpoint. 

* Runs as a standalone local Spark application, a spark driver application or is submittable to a Spark cluster.  
* Exposes a JDBC SQL endpoint using an embedded Hive Thrift server.
* Registers Parquet files as tables to allow SQL querying by non Spark applications over JDBC.
* Simple HTTP REST interface for table registration, de-registration and listing of registered tables.

Motivation: To allow non Spark based applications fluent in SQL to query Parquet backed tables in Spark over JDBC with the minimum of fuss.

### Build
```
$ git clone https://github.com/nwrs/spark-sql-server.git
$ cd spark-sql-server
$ mvn clean install
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
  -k, --sparkOpts  <opt=value,opt=value,...>       Additional Spark\Hive options.
  -t, --tableConfig  </path/to/TableConfig.conf>   Table configuration file.
  -h, --help                                       Show help message
```

### Running as Standalone Spark Driver Application
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
$ cat parquet-tables.config

#Example table config file
tweets=hdfs://localhost:9000/tweets/tweets.parquet
users=hdfs://localhost:9000/users/users.parquet 

```
## REST Interface

| Verb          | Path            |         Action             | Request Body | Response Body | Success Code |
| :------------- | :--------------- | :-------------------------- | :--- | :--- | :--- |
| GET           | /api/v1/tables         | List all registered tables | None | JSON | 200 |
| GET           | /api/v1/table/{table} | Get registration for a table | None | JSON |  200 |
| DELETE        | /api/v1/table/{table} | De-register (drop) a table| None | None | 202 |
| POST          | /api/v1/table          | Register a table | JSON | None | 202 |

Example JSON to register a table via POST:
```
{
    "table": "my_table_name",
    "file": "hdfs://hadoop-server:9000/data/tweet/tweets.parquet"
}
```

## Connecting to the JDBC Endpoint
 
* The default JDBC endpoint connection string is: "jdbc:hive2://localhost:10000/default"
* Driver class name: org.apache.hive.jdbc.HiveDriver
* Use at least version 2.1.0 of the Hive driver jar file, available [here](http://www.mvnrepository.com/artifact/org.apache.hive/hive-jdbc)
* In some circumstances hadoop-common-xxx.jar may required on the class path, choose the correct version to match your HDFS installation [here](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common)

See [here](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC) for more information on connecting to Hive via the JDBC driver.


