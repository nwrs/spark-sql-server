## Spark SQL Server Example

An example of a simple Spark driver application with an embedded Hive JDBC endpoint.

This approach can offer more flexibility than using the standard Hive server within Spark and is preferable when tables/views will be dynamically created/dropped or when the Hive connection is required in the same process as custom application code.

* Any SQL client that supports JDBC can connect using the Hive driver
* The JDBC connection string is of the format "jdbc:hive2://host:10000/default"
* For more info on connecting via JDBC via the Hive driver see [here]([https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC)