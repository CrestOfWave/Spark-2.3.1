
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

### 1.2 何时开启checkpoint

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
并且无法从早期checkpoint中重新启动升级前代码的信息。checkpoint信息本质上包含序列化的Scala / Java / Python对象，尝试使用新的修改类反序列化这些对象可能会导致错误。
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

## 2. 源码介绍

### 2.1  do checkpoint的入口

前面小结主要是讲了checkpoint的官网上整理的内容。下面就从源码的角度看看checkpoint。

首先是要回忆一下，spark streaming 的job生成的过程。是有个定时器按照批处理时间周期性的去调度生产job，当然这个在无窗口函数的时候是这样，有窗口函数的时候时间就是batch时间的整数倍了。
这段代码在 JobGenerator
```scala
  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
```

关于eventloop后面会在spark内部通讯机制里面讲到。这里用到的eventloop，在 JobGenerator start方法内部实现，代码如下：

````scala
eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()
````

processEvent(event)是具体处理事件的方法,主要有四种事件：

1. GenerateJobs：当前时间生成job；

2. ClearMetadata：当前时间清楚元数据；

3. DoCheckpoint：进行checkpoint；

4. ClearCheckpointData清除checkpoint 数据。

```scala
/** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }
```

鉴于，本文主要是讲解checkpoint的过程。所以，很细节的内容及eventloop的原理，浪尖在这里就不再讲解。


`generateJobs(time)`方法里，产生DoCheckpoint的事件

```scala
/** Generate jobs and perform checkpointing for the given `time`.  */
  private def generateJobs(time: Time) {
    // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
    // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
    ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
        PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
    }
    // 会调用 doCheckpoint(time, clearCheckpointDataLater)
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }
```

会通过eventloop，转而进入具体的处理函数`doCheckpoint(time, clearCheckpointDataLater)`
```scala
/** Perform checkpoint for the given `time`. */
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean) {
//    会先判断是否开启了checkpoint，然后时间间隔是否是checkpoint的整数倍。
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    } else if (clearCheckpointDataLater) {
      markBatchFullyProcessed(time)
    }
  }
```
首先会判断是否开启checkpoint，也即是shouldCheckpoint,该变量是个lazy型的变量，内容如下：
```scala
  // lazy变量，初始化之前需要设置checkpoint duration和checkpointDir。
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null
```

接着是，判断是否是到了checkpoint的周期。
```scala
(time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)
```

接着就进入了checkpoint操作，前面也说到了checkpoint主要分两个部分：

1. 更新checkpoint数据

2. 写chekpoint元数据。

````scala
ssc.graph.updateCheckpointData(time)
checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
````

### 2.2 checkpoint data

数据checkpoint，那必然是在DStream生成RDD的时候去做最靠谱。

当然，熟悉Spark Streaming源码的肯定知道，job的产生实际上伴随着RDD的生成。在JobGenerator方法内部`generateJobs(time: Time)`
```scala
graph.generateJobs(time) // generate jobs using allocated block

接着进入

  def generateJobs(time: Time): Seq[Job] = {
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    jobs
  }

```
会触发，每个输入流的generateJob的调用`outputStream.generateJob(time)`
```scala
*  使用给定的时间产生一个 Spark streaming 任务。
    */
  private[streaming] def generateJob(time: Time): Option[Job] = {
    getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => {
          val emptyFunc = { (iterator: Iterator[T]) => {} }
          context.sparkContext.runJob(rdd, emptyFunc)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }
```

那么，接下来就是终点了，RDD的生成`getOrCompute(time) `
````scala
 /**
   * Get the RDD corresponding to the given time; either retrieve it from cache
   * or compute-and-cache it.
   */
  private[streaming] final def getOrCompute(time: Time): Option[RDD[T]] = {
    // If RDD was already generated, then retrieve it from HashMap,
    // or else compute the RDD
    generatedRDDs.get(time).orElse {
      // Compute the RDD if time is valid (e.g. correct time in a sliding window)
      // of RDD generation, else generate nothing.
      if (isTimeValid(time)) {

        val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details. We need to have this call here because
          // compute() might cause Spark jobs to be launched.
          SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
            compute(time)
          }
        }

        rddOption.foreach { case newRDD =>
          // Register the generated RDD for caching and checkpointing
          if (storageLevel != StorageLevel.NONE) {
            newRDD.persist(storageLevel)
            logDebug(s"Persisting RDD ${newRDD.id} for time $time to $storageLevel")
          }
          if (checkpointDuration != null && (time - zeroTime).isMultipleOf(checkpointDuration)) {
            newRDD.checkpoint()
            logInfo(s"Marking RDD ${newRDD.id} for time $time for checkpointing")
          }
          generatedRDDs.put(time, newRDD)
        }
        rddOption
      } else {
        None
      }
    }
  }
````

可以看到在生成RDD后，假如开启了checkpoint并且到了checkpoint的时间，就会进行checkpoint,并且会把newRDD放入内存缓存
```scala
newRDD.checkpoint()

generatedRDDs.put(time, newRDD)
```

前面知识RDD的checkpoint，那么如何跟Spark Streaming的checkpoint的关联起来的呢？

回想前面的代码，其中 JobGenerator的 doCheckpoint 方法内部，其中有一行。

````scala
ssc.graph.updateCheckpointData(time)
````

这个是DStreamGraph的方法

```scala
def updateCheckpointData(time: Time) {
    logInfo("Updating checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
    logInfo("Updated checkpoint data for time " + time)
  }
```

可以看到，最终调用的是每个输出流的updateCheckpointData方法。主要目的是：

刷新已经checkpoint的RDD列表，该列表会随着stream的checkpoint一起被保存。 该实现仅会将已checkpoint的RDD的文件名保存到checkpointData。

```scala
  *  
    *
   */
  private[streaming] def updateCheckpointData(currentTime: Time) {
    logDebug(s"Updating checkpoint data for time $currentTime")
    checkpointData.update(currentTime)
    dependencies.foreach(_.updateCheckpointData(currentTime))
    logDebug(s"Updated checkpoint data for time $currentTime: $checkpointData")
  }
```
具体实现还是在 DStreamCheckpointData 的 update 方法中。
更新 DStream 的 checkpoint 数据。每次graph checkpoint发起的时候，都会调用。默认实现记录已保存DStream生成的RDD的checkpoint文件。

这里可以看到亲切的DStream的generatedRDDs对象。这样就完成了checkpoint 数据关联到了DStreamCheckpointData内部，也即是currentCheckpointFiles对象。

timeToCheckpointFile和timeToOldestCheckpointFileTime都被标记为了 @transient，数据会在DStreamCheckpointData序列话写入文件的时候丢失，在反序列化的时候创建了新的空对象。

关于transient请参考我的文章。[transient关键字详解](https://mp.weixin.qq.com/s/POBdZh79KO5B2dFTSJ-Cgg)
```scala
  def update(time: Time) {

    // Get the checkpointed RDDs from the generated RDDs
    //从 generatedRDDs 获取已checkpoint RDD
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))

    // Add the checkpoint files to the data to be serialized
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
      // Add the current checkpoint files to the map of all checkpoint files
      // This will be used to delete old checkpoint files
      timeToCheckpointFile ++= currentCheckpointFiles
      // Remember the time of the oldest checkpoint RDD in current state
      timeToOldestCheckpointFileTime(time) = currentCheckpointFiles.keys.min(Time.ordering)
    }
  }
```

### 2.3 checkpoint 元数据

元数据的checkpoint入口也是在 JobGenerator的 doCheckpoint 方法内部
```scala
checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
```

其实，可以分两个部分看：

1. Checkpoint类
    
2. CheckpointWriter类

#### 2.3.1 Checkpoint类

1. checkpoint的内容

```scala
  val master = ssc.sc.master
  val framework = ssc.sc.appName
  val jars = ssc.sc.jars
  val graph = ssc.graph
  val checkpointDir = ssc.checkpointDir
  val checkpointDuration = ssc.checkpointDuration
  val pendingTimes = ssc.scheduler.getPendingTimes().toArray
  val sparkConfPairs = ssc.conf.getAll
```

2. 序列化和反序列化的接口

serialize是在CheckpointWriter的write方法和streamingContext#start#validate方法内部调用，前者是进行checkpoint过程，后者主要是主要是为了验证DStream是否有不可序列化的方法。

```scala
 // Verify whether the DStream checkpoint is serializable
    if (isCheckpointingEnabled) {
      val checkpoint = new Checkpoint(this, Time(0))
      try {
        Checkpoint.serialize(checkpoint, conf)
      } catch {
        case e: NotSerializableException =>
          throw new NotSerializableException(
            "DStream checkpointing has been enabled but the DStreams with their functions " +
              "are not serializable\n" +
              SerializationDebugger.improveException(checkpoint, e).getMessage()
          )
      }
    }
```

deserialize在CheckpointReader的read方法内部调用，从checkpoint内部恢复。

#### 2.3.2 CheckpointWriter 类
该对象在构建的时候，其实内部构建了一个 [ThreadPoolExecutor](http://mp.weixin.qq.com/s?__biz=MzA3MDY0NTMxOQ==&mid=100001519&idx=1&sn=bceff23ea83bd44941ffec15255b66c1&chksm=1f38e5c7284f6cd10c40b0af9cd0414fabfa6c7a7d61efaf46087d5a60fc822f1461da2c1d10#rd)

````scala
val executor = new ThreadPoolExecutor(
    1, 1,
    0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](1000))
````

CheckpointWriteHandler是CheckpointWriter的内部类，实现了Runnable接口，主要作用如下：

1. 将checkpoint 写入临时文件
```scala
if (fs == null) {
            fs = new Path(checkpointDir).getFileSystem(hadoopConf)
          }
          // Write checkpoint to temp file
          fs.delete(tempFile, true) // just in case it exists
          val fos = fs.create(tempFile)
          Utils.tryWithSafeFinally {
            fos.write(bytes)
          } {
            fos.close()
          }
```

2. 备份存在的checkpoint文件
```scala
/ If the backup exists as well, just delete it, otherwise rename will fail
          if (fs.exists(checkpointFile)) {
            fs.delete(backupFile, true) // just in case it exists
            if (!fs.rename(checkpointFile, backupFile)) {
              logWarning(s"Could not rename $checkpointFile to $backupFile")
            }
          }
```

3. 将临时文件重命名

````scala
// Rename temp file to the final checkpoint file
          if (!fs.rename(tempFile, checkpointFile)) {
            logWarning(s"Could not rename $tempFile to $checkpointFile")
          }
````

4. 删除旧的checkpoint文件

```scala
// Delete old checkpoint files
          val allCheckpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs))
          if (allCheckpointFiles.size > 10) {
            allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach { file =>
              logInfo(s"Deleting $file")
              fs.delete(file, true)
            }
          }
```

5. 清除checkpoint数据

1-4步骤都是Checkpoint对象序列化，然后写入checkpoint文件的过程。前面也说到了checkpoint数据 的过程但是就是没有checkpoint 数据删除的过程。

```scala
   jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater)

```

经过eventloop滞后进入了`clearCheckpointData`方法,在里面对checkpoint数据进行了清除。

```scala
/** Clear DStream checkpoint data for the given `time`. */
  private def clearCheckpointData(time: Time) {
    ssc.graph.clearCheckpointData(time)

    // All the checkpoint information about which batches have been processed, etc have
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
    markBatchFullyProcessed(time)
  }
```

### 2.4 从checkpoint恢复

从checkpoint恢复，应该也包括两个部分：

1. 恢复元数据

2. 恢复数据

#### 2.4.1 恢复元数据
入口必然是，第一节所说的
```scala
StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

def getOrCreate(
      checkpointPath: String,
      creatingFunc: () => StreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    ): StreamingContext = {
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), hadoopConf, createOnError)
    checkpointOption.map(new StreamingContext(null, _, null)).getOrElse(creatingFunc())
  }

```

CheckpointReader的read方法内部就是调用`Checkpoint.deserialize(fis, conf)`反序列化得到checkpoint对象的过程。

然后就是利用恢复的checkpoint对象，创建一个新的StreamingContext
```scala
    checkpointOption.map(new StreamingContext(null, _, null)).getOrElse(creatingFunc())
```

Checkpoint内容，那么必然也会用反序列化的checkpoint对象，来恢复这些内容。
```scala
  val master = ssc.sc.master
  val framework = ssc.sc.appName
  val jars = ssc.sc.jars
  val graph = ssc.graph
  val checkpointDir = ssc.checkpointDir
  val checkpointDuration = ssc.checkpointDuration
  val pendingTimes = ssc.scheduler.getPendingTimes().toArray
  val sparkConfPairs = ssc.conf.getAll
```

#### 2.4.2 恢复数据

利用checkpoint对象构建StreamingContext的时候
```scala
//  加假如有checkpoint就从checkpoint恢复，没有checkpoint就创建新的
  private[streaming] val graph: DStreamGraph = {
    if (isCheckpointPresent) {
      _cp.graph.setContext(this)
      _cp.graph.restoreCheckpointData()
      _cp.graph
    } else {
      require(_batchDur != null, "Batch duration for StreamingContext cannot be null")
      val newGraph = new DStreamGraph()
      newGraph.setBatchDuration(_batchDur)
      newGraph
    }
  }
```
其中就有恢复数据的过程
```scala
cp.graph.restoreCheckpointData()
```
restoreCheckpointData方法实现
```scala
def restoreCheckpointData() {
    logInfo("Restoring checkpoint data")
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
    }
    logInfo("Restored checkpoint data")
  }
```

在DStream的 restoreCheckpointData 方法,该方法主要目的是：
从checkpoint数据中恢复generatedRDDs内部的RDD，

```scala
 private[streaming] def restoreCheckpointData() {
    if (!restoredFromCheckpointData) {
      // Create RDDs from the checkpoint data
      logInfo("Restoring checkpoint data")
      checkpointData.restore()
      dependencies.foreach(_.restoreCheckpointData())
      restoredFromCheckpointData = true
      logInfo("Restored checkpoint data")
    }
  }
```

恢复的过程是DStreamCheckpointData的restore方法

```scala
  def restore() {
    // Create RDDs from the checkpoint data
    currentCheckpointFiles.foreach {
      case(time, file) =>
        logInfo("Restoring checkpointed RDD for time " + time + " from file '" + file + "'")
        dstream.generatedRDDs += ((time, dstream.context.sparkContext.checkpointFile[T](file)))
    }
  }
```

currentCheckpointFiles不就是2.2小结说到的currentCheckpointFiles，写入到checkpoint的内容么。

这就结束了整个恢复的过程