Spark SQL
=========

该模型允许你使用SQL或者DataFrame或DataSet进行关系型查询
Spark SQL划分为四个子项目:
 - Catalyst (sql/catalyst) - 解析关系操作算子和表达式的语法树的实现框架
 - Execution (sql/core) - 代表planner / execution engine 用来转化Catalyst的逻辑查询计划为park RDDs.  该部件也允许用户将 RDDs and Parquet files转化为表，来进行查询分析.
 - Hive Support (sql/hive) - 包含了一个SqlContext扩展HiveContext，允许使用 HiveQL 的子集and 允许使用 Hive SerDes去访问hive metastore的数据. 也可以使用Hive 的UDFs, UDAFs, and UDTFs来进行查询分析.
 - HiveServer and CLI support (sql/hive-thriftserver) - Includes support for the SQL CLI (bin/spark-sql) and a HiveServer2 (for JDBC/ODBC) compatible server.

Running `sql/create-docs.sh` generates SQL documentation for built-in functions under `sql/site`.
