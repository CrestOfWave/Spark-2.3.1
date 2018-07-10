Spark SQL
=========

该模型允许你使用SQL或者DataFrame或DataSet进行关系型查询
Spark SQL划分为四个子项目:
 - Catalyst (sql/catalyst) - 解析关系操作算子和表达式的语法树的实现框架
 - Execution (sql/core) - 代表planner / execution engine 用来转化Catalyst的逻辑查询计划为park RDDs.  该部件也允许用户将 RDDs and Parquet files转化为表，来进行查询分析.
 - Hive Support (sql/hive) - 包含了一个SqlContext扩展HiveContext，允许使用 HiveQL 的子集and 允许使用 Hive SerDes去访问hive metastore的数据. 也可以使用Hive 的UDFs, UDAFs, and UDTFs来进行查询分析.
 - HiveServer and CLI support (sql/hive-thriftserver) - Includes support for the SQL CLI (bin/spark-sql) and a HiveServer2 (for JDBC/ODBC) compatible server.

Running `sql/create-docs.sh` generates SQL documentation for built-in functions under `sql/site`.

如果使用选择使用SQL进行查询，首先需要将SQL解析成spark中的抽象语法树（AST）。在spark中是借助开源的antlr4库来解析的。Spark SQL的语法规则文件是：SqlBase.g4。该文件以及生成的相关文件截图如下。



在生成的文件中SqlBaseBaseListener和SqlBaseBaseVistor分别代表两种遍历AST的方法，在spark中主要用了visitor模式。

接下来，将看一下spark中，当使用spark.sql("select *** from ...")时，sql怎么解析成spark内部的AST的？

1，用户调用的spark.sql的入口是sparkSession中sql函数，该函数最终返回DataFrame（DataSet[Row]），sql的解析的过程主要是在

sessionState.sqlParser.parsePlan(sqlText)中发生的。
def sql(sqlText: String): DataFrame = {
  Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
}
2，调用到parsePlan，将调用parse函数，传入的两个参数分为：sql语句，sqlBaseParse到LogicalPlan的一个函数。
override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
  astBuilder.visitSingleStatement(parser.singleStatement()) match {
    case plan: LogicalPlan => plan
    case _ =>
      val position = Origin(None, None)
      throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
  }
}
3，在parse函数中，首先构造SqlBaseLexer词法分析器，接着构造Token流，最终SqlBaseParser对象，然后一次尝试用不同的模式去进行解析。最终将执行parsePlan中传入的函数。



4，在步骤2中，astBuilder是SparkSqlAstBuilder的实例，在将Antlr中的匹配树转换成unresolved logical plan中，它起着桥梁作用。

astBuilder.visitSingleStatement使用visitor模式，开始匹配SqlBase.g4中sql的入口匹配规则：
singleStatement
 : statement EOF
 ;
递归的遍历statement，以及其后的各个节点。在匹配过程中，碰到叶子节点，就将构造Logical Plan中对应的TreeNode。如当匹配到

singleTableIdentifier
 : tableIdentifier EOF
 ;
规则时(单表的标识符)。即调用的函数如下：

override def visitSingleTableIdentifier(
    ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
  visitTableIdentifier(ctx.tableIdentifier)
}
可以看到将递归遍历对应的tableIdentifier，tableIdentifier的定义和遍历规则如下：

tableIdentifier
 : (db=identifier '.')? table=identifier
 ;
override def visitTableIdentifier(
    ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
  TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
}
可以看到当匹配到tableIdentifier，将直接生成TableIdentifier对象，而该对象是TreeNode的一种。经过类似以上的过程，匹配结束后整个spark内部的抽象语法树也就建立起来了。