
# checkpoint

## 1. checkpoint官网翻译

### 1.1 checkpoint简介
流应用程序必须7*24小时运行，因此必须能够适应与应用程序逻辑无关的故障（例如，系统故障，JVM崩溃等）。 
为了实现这一点，Spark Streaming需要将足够的信息checkpoint到容错存储系统，以便它可以从故障中恢复。 checkpoint有两种类型的数据。

* 元数据checkpoint - 将定义流式计算的信息保存到容错存储（如HDFS）。这用于从运行流应用程序的driver节点的故障中恢复（稍后详细讨论）。元数据包括：
 
  * 配置 - 用于创建流应用程序的配置。
  * DStream操作 - 定义流应用程序的DStream操作集。
  * 未完成的批次 - 未完成的批次的job队列。
 
* 数据checkpoint - 将生成的RDD保存到可靠的存储。在一些跨多个批次组合数据的有状态转换中，这是必需的。在这种转换中，生成的RDD依赖于先前批次的RDD，这导致依赖链的长度随时间增加。
为了避免恢复时间的无限增加（故障恢复时间与依赖链成比例），有状态转换的中RDD周期性地checkpoint到可靠存储（例如HDFS）以切断依赖链。

总而言之，元数据checkpoint主要用于从driver故障中恢复，而如果使用有状态转换操作，也需要数据或RDD 进行checkpoint。

### 1.2 合适使能checkpoint

必须为具有以下任何要求的应用程序启用checkpoint：

* 有状态转换的算子 - 如果在应用程序中使用updateStateByKey或reduceByKeyAndWindow），则必须提供checkpoint目录以允许定期RDD checkpoint。

* 从driver故障中恢复 - 元数据checkpoint用于使用进度信息进行恢复。

请注意，可以在不启用checkpoint的情况下运行没有上述有状态转换的简单流应用程序。 在这种情况下，driver故障的恢复也不完整（某些已接收但未处理的数据可能会丢失）。 
这通常是可以接受的，并且有许多以这种方式运行Spark Streaming应用程序。 对非Hadoop环境的支持希望将在未来得到改善。

### 1.3 如何配置 checkpoint

可以通过在容错，可靠的文件系统（例如，HDFS，S3等）中设置目录来启用checkpoint，在目录中将保存checkpoint信息。 
通过使用streamingContext.checkpoint（checkpointDirectory）来完成设置。 
此外，如果要使应用程序从driver故障中恢复，则应重写流应用程序以使其具有以下行为。

* 当程序第一次启动时，它将创建一个新的StreamingContext，设置所有流然后调用start（）。

* 在失败后重新启动程序时，它将从checkpoint目录中的checkpoint数据重新创建StreamingContext。

````scala
// Function to create and setup a new StreamingContext
def functionToCreateContext(): StreamingContext = {
  val ssc = new StreamingContext(...)   // new context
  val lines = ssc.socketTextStream(...) // create DStreams
  ...
  ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
  ssc
}

// Get StreamingContext from checkpoint data or create a new one
val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

// Do additional setup on context that needs to be done,
// irrespective of whether it is being started or restarted
context. ...

// Start the context
context.start()
context.awaitTermination()
````

如果checkpointDirectory存在，则将从checkpoint数据重新创建上下文。如果该目录不存在（即，第一次运行），则将调用函数functionToCreateContext以创建新上下文并设置DStream。


除了使用getOrCreate之外，还需要确保driver进程在失败时自动重新启动。这只能通过应用程序部署的集群管理器来完成，比如yarn。

请注意，RDD的checkpoint会导致写入可靠存储的开销。这可能导致RDD被checkpoint的那些批次的处理时间增加。因此，需要谨慎设置checkpoint的间隔。
在小批量（例如1秒）下，每批次checkpoint可能会显着降低操作吞吐量。相反，checkpoint太过不频繁会导致血统链增长和任务大小增加，这可能会产生不利影响。
对于需要RDDcheckpoint的有状态转换，默认时间间隔是批处理间隔的倍数，至少为10秒。可以使用dstream.checkpoint（checkpointInterval）进行设置。
通常推荐，checkpoint间隔设置为DStream的5-10个滑动间隔（不是仅限于batch，还有windows的滑动间隔).

### 1.4 累加器，广播变量和checkpoint

spark streaming中的广播变量和累加器无法从checkpoint中恢复。如果启用了checkpoint并使用累加器或广播变量，则必须为累加器和广播变量创建lazy实例化的单例实例，
以便在driver重新启动失败后重新实例化它们。 

举个例子

```scala
object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

object DroppedWordsCounter {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}

wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
  // Get or register the blacklist Broadcast
  val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
  // Get or register the droppedWordsCounter Accumulator
  val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
  // Use blacklist to drop words and use droppedWordsCounter to count them
  val counts = rdd.filter { case (word, count) =>
    if (blacklist.value.contains(word)) {
      droppedWordsCounter.add(count)
      false
    } else {
      true
    }
  }.collect().mkString("[", ", ", "]")
  val output = "Counts at time " + time + " " + counts
})
```


### 1.5 升级应用程序代码

如果需要使用新的应用程序代码升级正在运行的Spark Streaming应用程序，则有两种可能的机制。

1. 升级的Spark Streaming应用程序启动并与现有应用程序并行运行。一旦新的程序（接收与旧的数据相同的数据）已经预热并准备好最合适的时间，旧应用可以被下架了。
请注意，这仅可以用于数据源支持同时将数据发送到两个地放（即早期和升级的应用程序）。

2. 温柔地关闭现有应用程序（StreamingContext.stop或JavaStreamingContext.stop这两个API文档里有温柔停止应用程序的参数详解），以确保在关闭之前完全处理已接收的数据。
然后可以启动升级的应用程序，该应用程序将从早期应用程序停止的同一位置开始处理。请注意，这只能通过支持源端缓冲的输入源（如Kafka和Flume）来完成，因为在前一个应用程序关闭且升级的应用程序尚未启动时需要缓冲数据。
并且无法从早期checkpoint中重新启动升级前代码的信息。checkpoint信息基本上包含序列化的Scala / Java / Python对象，而且尝试使用新的修改类反序列化这些对象可能会导致错误。
在这种情况下，要么使用不同的checkpoint目录启动升级的应用程序，要么删除以前的checkpoint目录。
StreamingContext.stop 方法如下：
```scala
def
stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit
 Permalink
Stop the execution of the streams, with option of ensuring all received data has been processed.

stopSparkContext
if true, stops the associated SparkContext. The underlying SparkContext will be stopped regardless of whether this StreamingContext has been started.

stopGracefully
if true, stops gracefully by waiting for the processing of all received data to be completed
```

当然，一个配置参数也可以设置：

```scala
spark.streaming.stopGracefullyOnShutdown	false	If true, Spark shuts down the StreamingContext gracefully on JVM shutdown rather than immediately.
```




















