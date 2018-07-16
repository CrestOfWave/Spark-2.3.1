## Spark SQL

------

该模型允许你使用SQL或者DataFrame或DataSet进行关系型查询
Spark SQL划分为四个子项目:
 - Catalyst (sql/catalyst) - 解析关系操作算子和表达式的语法树的实现框架
 - Execution (sql/core) - 代表planner / execution engine 用来转化Catalyst的逻辑查询计划为park RDDs.  该部件也允许用户将 RDDs and Parquet files转化为表，来进行查询分析.
 - Hive Support (sql/hive) - 包含了一个SqlContext扩展HiveContext，允许使用 HiveQL 的子集and 允许使用 Hive SerDes去访问hive metastore的数据. 也可以使用Hive 的UDFs, UDAFs, and UDTFs来进行查询分析.
 - HiveServer and CLI support (sql/hive-thriftserver) - Includes support for the SQL CLI (bin/spark-sql) and a HiveServer2 (for JDBC/ODBC) compatible server.

Running `sql/create-docs.sh` generates SQL documentation for built-in functions under `sql/site`.

### Spark SQL的执行计划

------

![image](https://github.com/CrestOfWave/Spark-2.3.1/blob/master/sql/SparkSQL执行计划.jpg)

总体执行流程如下：从提供的输入API（SQL，Dataset， dataframe）开始，依次经过unresolved逻辑计划，解析的逻辑计划，优化的逻辑计划，物理计划，然后根据cost based优化，选取一条物理计划进行执行。从unresolved logical plan开始， sql的查询是通过抽象语法树（AST）来表示的，所以以后各个操作都是对AST进行的等价转换操作。 针对以上过程作如下几点说明：

1，编程接口：通过像df.groupBy("age")这样的Dataset接口构造查询过程，抽象语法树（AST）会自动建立。而通过“SELECT name, count(age) FROM people where age > 21 group by name” 这样的sql语句进行查询时，需要增加一个步骤是，需要将SQL解析成AST（spark 2.2中目前是借助于antlr4来做的，具体见后面分析）。

2，经过步骤1后，我们可以得到unresolved logical plan，此时像以上sql中的name，count（age），people都是unresolved attribute，relation等，他们是AST树TreeNode的一中类型，但是他们是不能被计算的（实现了Unevaluable接口）。

3，unresolved logical plan通过Analyzer模块定义的一系列规则，将步骤2中的unresolved的attribute，relation借助catalog去解析，如将之前提到的unresolved attribute转换成resolved attribute。此时，如果sql中某个表不存在或者列和表不对应，在此阶段便可以发现。Analyzer定义一系列部分规则如下：



4，解析成resolved logical plan以后，通过一系列优化规则会将resolved logical plan的AST转化成optimized logical plan的AST。这些优化包括基于规则和代价的优化，比如谓词下推，列值裁剪等。



5，AST到了optimized logical plan以后，利用如下的策略将逻辑计划转化成物理计划，物理计划是可以执行的计划。当有相关的action操作时，物理计划便可以执行。

### SQL解析的过程

------


如果选择使用SQL进行查询，首先需要将SQL解析成spark中的抽象语法树（AST）。在spark中是借助开源的antlr4库来解析的。
Spark SQL的语法规则文件是：SqlBase.g4。

![image](https://github.com/CrestOfWave/Spark-2.3.1/blob/master/sql/SparkSqlparser.jpg)


在生成的文件中SqlBaseBaseListener和SqlBaseBaseVistor分别代表两种遍历AST的方法，在spark中主要用了visitor模式。

接下来，将看一下spark中，当使用spark.sql("select *** from ...")时，sql怎么解析成spark内部的AST的？

1，用户调用的spark.sql的入口是sparkSession中sql函数，该函数最终返回DataFrame（DataSet[Row]），sql的解析的过程主要是在sessionState.sqlParser.parsePlan(sqlText)中发生的。
```scala

def sql(sqlText: String): DataFrame = {
  Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
}
```
2，调用到parsePlan，将调用parse函数，传入的两个参数分为：sql语句，sqlBaseParse到LogicalPlan的一个函数。
```scala
override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
  astBuilder.visitSingleStatement(parser.singleStatement()) match {
    case plan: LogicalPlan => plan
    case _ =>
      val position = Origin(None, None)
      throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
  }
}
```
3，在parse函数中，首先构造SqlBaseLexer词法分析器，接着构造Token流，最终SqlBaseParser对象，然后一次尝试用不同的模式去进行解析。最终将执行parsePlan中传入的函数。
```scala
 protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}
```


4，在步骤2中，astBuilder是SparkSqlAstBuilder的实例，在将Antlr中的匹配树转换成unresolved logical plan中，它起着桥梁作用。
astBuilder.visitSingleStatement使用visitor模式，开始匹配SqlBase.g4中sql的入口匹配规则：
```scala
singleStatement
 : statement EOF
 ;
```
 
递归的遍历statement，以及其后的各个节点。在匹配过程中，碰到叶子节点，就将构造Logical Plan中对应的TreeNode。如当匹配到
```scala
singleTableIdentifier
 : tableIdentifier EOF
 ;
 
```
规则时(单表的标识符)。即调用的函数如下：
```scala
override def visitSingleTableIdentifier(
    ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
  visitTableIdentifier(ctx.tableIdentifier)
}
```
可以看到将递归遍历对应的tableIdentifier，tableIdentifier的定义和遍历规则如下：
```scala
tableIdentifier
 : (db=identifier '.')? table=identifier
 ;
 
 
override def visitTableIdentifier(
    ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
  TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
}
```

可以看到当匹配到tableIdentifier，将直接生成TableIdentifier对象，而该对象是TreeNode的一种。
经过类似以上的过程，匹配结束后整个spark内部的抽象语法树也就建立起来了。

### 逻辑执行计划到物理计划的生成过程
我们这个过程主要是以Dataset的head（n）方法为例。head(n)方法代码如下：
```scala
  /**
   * Returns the first `n` rows.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   *
   * @group action
   * @since 1.6.0
   */
  def head(n: Int): Array[T] = withAction("head", limit(n).queryExecution)(collectFromPlan)
```
其实，实际上他是利用limit方法构建了一个Dataset，然后获取queryExecution，再获取executedPlan，给collectFromPlan。这块代码如下：
```scala
limit(n).queryExecution

  * Returns a new Dataset by taking the first `n` rows. The difference between this function
   * and `head` is that `head` is an action and returns an array (by triggering query execution)
   * while `limit` returns a new Dataset.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def limit(n: Int): Dataset[T] = withTypedPlan {
    Limit(Literal(n), planWithBarrier)
  }
  
  /** A convenient function to wrap a logical plan and produce a Dataset. */
    @inline private def withTypedPlan[U : Encoder](logicalPlan: LogicalPlan): Dataset[U] = {
      Dataset(sparkSession, logicalPlan)
    }

```
head（n）实现处withAction代码如下：
```scala
/**
   * Wrap a Dataset action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
    * 封装一个Dataset，主要是跟踪QueryExecution和时间消耗
   */
  private def withAction[U](name: String, qe: QueryExecution)(action: SparkPlan => U) = {
    try {
      qe.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = SQLExecution.withNewExecutionId(sparkSession, qe) {
        action(qe.executedPlan)
      }
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, qe, end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }
```
这块代码调用逻辑有点绕吧，实际上就是scala的编程风格。collectFromPlan该段代码不去深入，先说如何从物理计划到逻辑计划的。

在withAction里面，qe.executedPlan这实际上就是逻辑执行计划到物理执行计划转换的入口。qe就是QueryExecution。
执行计划相关的都是lazy形式的变量，在实际调用的时候才会真正实例化。

QueryExecution类中重点关注的是这些lazy变量，只会在executedPlan或者tordd调用的时候才会真正执行实例化。
```scala
/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
  *
  * 使用spark做关系查询的主要工作流，设计允许开发这方便使用查询执行的中间语义。
  * 然而这并不是一个开放的类，应该避免修改函数名，来修改他们，因为大量的开发者使用该特性进行debug
 */
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

//  解析逻辑计划为解析过的逻辑计划
  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

//  使用缓存的数据
  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

//  优化逻辑执行计划
  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

//  生成物理执行计划 BaseSessionStateBuilder 内部构建SparkPlaner，并生成传递给 SessionState
  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()
```

物理执行计划实际上是通过Plannner.plan过程生成的，也即是QueryPlanner
```scala
def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // 收集候选物理计划.
    val candidates = strategies.iterator.flatMap(_(plan))

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    // 替换
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p == placeholder => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }
```

### codegen过程

------

逻辑执行计划到物理执行计划转化的过程是在SparkPlanner内，物理执行计划到codegen是在每一个物理执行计划内部构建的。

codegen技术是用scala的字符串插值特性生成源码，然后使用Janino，编译成java字节码。

本小结是讲codegen的过程，以SortExec为例，我们可以查看其具体执行的方法，doExecutor

```scala

  protected override def doExecute(): RDD[InternalRow] = {
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val sortTime = longMetric("sortTime")

    child.execute().mapPartitionsInternal { iter =>
    创建sorter
      val sorter = createSorter()

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      sortTime += sorter.getSortTimeNanos / 1000000
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
  }
```
这个里面最重要的方法是createSorter,我们进入查看，可见其代码如下：
```scala
 def createSorter(): UnsafeExternalRowSorter = {
 codegen的具体位置
    val ordering = newOrdering(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
          UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = UnsafeExternalRowSorter.create(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    if (testSpillFrequency > 0) {
      sorter.setTestSpillFrequency(testSpillFrequency)
    }
    sorter
  }
```
codegen的入口就是，newOrdering(sortOrder, output)->GenerateOrdering.generate(order, inputSchema),进入
GenerateOrdering.generate(order, inputSchema)最终会跳转到GenerateOrdering的create(canonicalize(expressions))，
```scala
  /**
   * Creates a code gen ordering for sorting this schema, in ascending order.
   */
  def create(schema: StructType): BaseOrdering = {
    create(schema.zipWithIndex.map { case (field, ordinal) =>
      SortOrder(BoundReference(ordinal, field.dataType, nullable = true), Ascending)
    })
  }
```
下面，就是codegen的过程
```scala
  protected def create(ordering: Seq[SortOrder]): BaseOrdering = {
    val ctx = newCodeGenContext()
    val comparisons = genComparisons(ctx, ordering)
    使用scala 字符串插值的形式定义java source code
    val codeBody = s"""
      public SpecificOrdering generate(Object[] references) {
        return new SpecificOrdering(references);
      }

      class SpecificOrdering extends ${classOf[BaseOrdering].getName} {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificOrdering(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public int compare(InternalRow a, InternalRow b) {
          $comparisons
          return 0;
        }

        ${ctx.declareAddedFunctions()}
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated Ordering by ${ordering.mkString(",")}:\n${CodeFormatter.format(code)}")

    使用java Janino 将java sourcecode 编译成java class
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BaseOrdering]
  }
}
```

其中CodeGenerator.compile(code)代码如下：
```scala
def compile(code: CodeAndComment): (GeneratedClass, Int) = try {
    cache.get(code)
  } catch {
    // Cache.get() may wrap the original exception. See the following URL
    // http://google.github.io/guava/releases/14.0/api/docs/com/google/common/cache/
    //   Cache.html#get(K,%20java.util.concurrent.Callable)
    case e @ (_: UncheckedExecutionException | _: ExecutionError) =>
      throw e.getCause
  }
```
其中cache类型是localcache的定义如下：
```scala
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[CodeAndComment, (GeneratedClass, Int)]() {
        override def load(code: CodeAndComment): (GeneratedClass, Int) = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
          CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })
```
localcache的特点就是，调用cache.get,会有以下动作：
- 1，存在，就直接加载
- 2，不存在就调用cacheloader的load方法生成，并且会将生成的内容加入cache

正在source编译为java class方法的入口是doCompile(code)：
```scala
 /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: CodeAndComment): (GeneratedClass, Int) = {
    val evaluator = new ClassBodyEvaluator()

    // A special classloader used to wrap the actual parent classloader of
    // [[org.codehaus.janino.ClassBodyEvaluator]] (see CodeGenerator.doCompile). This classloader
    // does not throw a ClassNotFoundException with a cause set (i.e. exception.getCause returns
    // a null). This classloader is needed because janino will throw the exception directly if
    // the parent classloader throws a ClassNotFoundException with cause set instead of trying to
    // find other possible classes (see org.codehaus.janinoClassLoaderIClassLoader's
    // findIClass method). Please also see https://issues.apache.org/jira/browse/SPARK-15622 and
    // https://issues.apache.org/jira/browse/SPARK-11636.
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("org.apache.spark.sql.catalyst.expressions.GeneratedClass")
    evaluator.setDefaultImports(Array(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName
    ))
    evaluator.setExtendedClass(classOf[GeneratedClass])

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      s"\n${CodeFormatter.format(code)}"
    })

    val maxCodeSize = try {
      evaluator.cook("generated.java", code.body)
      updateAndGetCompilationStats(evaluator)
    } catch {
      case e: InternalCompilerException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        val maxLines = SQLConf.get.loggingMaxLinesForCodegen
        logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
        throw new InternalCompilerException(msg, e)
      case e: CompileException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        val maxLines = SQLConf.get.loggingMaxLinesForCodegen
        logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
        throw new CompileException(msg, e.getLocation)
    }

    (evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass], maxCodeSize)
  }
```
