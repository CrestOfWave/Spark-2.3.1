
# reduceByKey VS GroupByKey

## 1. ReduceByKey

针对每一个key，合并其value。也会在map端进行聚合，该聚合功能就很类似mapreduce的combine。

```scala
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }


  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }


  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
  }
```

## 2. GroupByKey

故名思意，其作用就是将RDD的数据按照每个key进行分组到一个序列中去。可以通过传入一个partitioner，来指定结果RDD的分区策略。
每个分组中元素的顺序是无法保证的，每次执行顺序可能都不一样。

请注意： 该操作代价 是相当高的。所以，假如你分组的目的是想进行聚合操作，使用PairRDDFunctions.aggregateByKey和PairRDDFunctions.reduceByKey会有更好的性能。

再注意：当前的实现方法中，groupByKey需要将任意key的所有key-value对保存到内存中。如果，一个key的values过多，可能会导致OutOfMemoryError。
```scala
  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
//    groupByKey不需要开启map端合并，因为map端合并并不会减少shuffle的数据量，而且map端的所有数据都会被插入一张hash 表，这会使得更多的对象进入老年代。
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }

  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(new HashPartitioner(numPartitions))
  }
```

## 3. 简单对比

1. 返回值

reduceByKey返回的是`RDD[(K, V)]`,value的类型和传入的func的参数类型一致，也即是和要聚合的value类型一致。

groupByKey返回的是`RDD[(K, Iterable[V])]`，返回值的类型是关于欲求value类型V类型的迭代器。

2. 都调用了 combineByKeyWithClassTag 方法
```scala
def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] 
```
传入参数对比：

groupbykey#createCombiner是一个v-->CompactBuffer(v)的方法：(v: V) => CompactBuffer(v)
groupbykey#mergeValue是 (buf: CompactBuffer[V], v: V) => buf += v
groupbykey#mergeCombiners是(c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2

reduceByKey#createCombiner是(v: V) => v
reduceByKey#mergeValue是传入的聚合函数
reduceByKey#mergeCombiners是传入的聚合函数

3. map端聚合

groupbykey设置mapSideCombine = false，而reducebykey采用了默认端true值。

## 4. 说说combineByKeyWithClassTag

该方法标注为Experimental。

使用聚合函数针对每一个key进行聚合的通用方法。将RDD[(K, V)]转化为RDD[(K, C)]，C是聚合后的类型。

用户需要指定下面几个方法：

1. createCombiner 将V类型转化为C类型，比如，将单个value转化为一个list。

2. mergeValue 将V合并到C

3. mergeCombiners 将两个C合并为一个C

### 4.1 代码解析

1. 首先是key类型判断

map端聚合操作开启和使用hashpartitioner，这两种情况是不支持key的类型为数组的。

```scala
   if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("HashPartitioner cannot partition array keys.")
      }
    }
```

2. 定义一个Aggregator

```scala
val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
      
      
该类具体方法如下：
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}

```

combineValuesByKey和combineCombinersByKey会根据是否开启map端聚合在BlockStoreShuffleReader[K, C] 的read方法中被调用。

3. 分区数无变化的，也即是不产生shuffle

可能会好奇两个分区器对象比较地意义，在这里(HashPartitioner)就是比较分区数，每个partition要实现hashcode和equals方法
```scala
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    }
```
直接是针对整个分区迭代器进行聚合，不产生shuffle.

```scala
aggregator.combineValuesByKey(iter, context)

def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

```

4. 存在shuffle
构建了一个ShuffledRDD。

```scala
new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
```

那么这里的代码阅读就要分两个部分了。

* 1 map端-ShuffleMapTask
这里只贴主要的代码
```scala
val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } 
```
ShuffleManager现在的版本只有一种，也即是SortShuffleManager，1.6的时候还有一种叫做，HashShuffleManager。

Shuffle种类目前支持的三种，在SortShuffleManager的registerShuffle方法内部可以看到。详细对比，后面分析shuffle的时候会说到。
```scala
override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }
```

本小节要分析的内容是：
```scala
writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
```

在write方法内部，以SortShuffleWriter为例

```scala
/** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }
```

可以看到，它会根据是否开启map端聚合，来决定是否在map端进行聚合。


* 2 reduce端-ShuffledRDD的

shuffledRDD的Compute方法。
```scala
   val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
```
其中，构建的reader是BlockStoreShuffleReader，进入read,其中有一段代码，会根据是否开启map端聚合，调用Aggregator的不同方法(combineValuesByKey or combineCombinersByKey)。

```scala
val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    }
```

* 3 ExternalSorter 和 ExternalAppendOnlyMap

shuffle write的时候有个非常重要的类-ExternalSorter

shuffle read的时候也有哥非常重要的类-ExternalAppendOnlyMap

## 5. 哪个好

存在既有价值。

GroupBykey，分组排序或者其它分组非聚合操作，比如按照用户的ID，分组，然后按照再按照时间进行排序，这张groupbykey很适用。

reduceBykey，分组聚合。


