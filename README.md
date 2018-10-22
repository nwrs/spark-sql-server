## Spark SQL Server Example

An example of a simple Spark driver application with an embedded Hive JDBC endpoint.

* Offers more flexibility than using the standard Hive server within Spark. 
* Preferable when tables/views will be dynamically created/dropped. 
* Useful when the Hive connection is required in the same process as custom application code.

