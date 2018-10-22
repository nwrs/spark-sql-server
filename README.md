## Spark SQL Server Example

An example of a simple Spark driver application with an embedded Hive JDBC endpoint.

This approach can offer more flexibility than using the standard Hive server within Spark. This approach can be preferable when tables/views will be dynamically created/dropped, it's also useful when the Hive connection is required in the same process as custom application code.

