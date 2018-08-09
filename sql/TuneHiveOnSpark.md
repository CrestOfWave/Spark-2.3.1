
# hive on spark

hive on spark 性能远比hive on mr 要好，而且提供了一样的功能。用户的sql 无需修改就可以直接运行于hive on spark。
udf函数也是全部支持。

本文主要是想将hive on spark 在运行于yarn模式的情况下如何调优。

下文举例讲解的yarn节点机器配置，假设有32核，120GB内存。

## yarn配置

`yarn.nodemanager.resource.cpu-vcores`和`yarn.nodemanager.resource.memory-mb`,这两个参数决定这集群资源管理器能够有多少资源用于运行yarn上的任务。
这两个参数的值是由机器的配置及同时在机器上运行的其它进程共同决定。本文假设仅有hdfs的datanode和yarn的nodemanager运行于该节点。

1. 配置cores

基本配置是datanode和nodemanager各一个核，操作系统两个核，然后剩下28核配置作为yarn资源。也即是
`yarn.nodemanager.resource.cpu-vcores=28`
2. 配置内存
对于内存，预留20GB给操作系统，datanode，nodemanager，剩余100GB作为yarn资源。也即是
`yarn.nodemanager.resource.memory-mb=100*1024`

## spark配置

给yarn分配资源以后，那就要想着spark如何使用这些资源了，主要配置对象：

    execurtor 和driver内存，executro配额，并行度。

1. executor内存

设置executor内存需要考虑如下因素:
    
* executor内存越多，越能为更多的查询提供map join的优化。由于垃圾回收的压力会导致开销增加。
    
* 某些情况下hdfs的 客户端不能很好的处理并发写入，所以过多的核心可能会导致竞争。

为了最大化使用core，建议将core设置为4，5，6（多核心会导致并发问题，所以写代码的时候尤其是静态的链接等要考虑并发问题）具体分配核心数要结合yarn所提供的核心数。
由于本文中涉及到的node节点是28核，那么很明显分配为4的化可以被整除，`spark.executor.cores`设置为4 不会有多余的核剩下,设置为5，6都会有core剩余。
`spark.executor.cores=4`，由于总共有28个核，那么最大可以申请的executor数是7。总内存处以7，也即是 100/7，可以得到每个executor约14GB内存。

要知道 `spark.executor.memory` 和`spark.executor.memoryOverhead` 共同决定着 executor内存。建议 `spark.executor.memoryOverhead`站总内存的 15%-20%。
那么最终 `spark.executor.memoryOverhead=2 G ` 和` spark.executor.memory=12 G`

根据上面的配置的化，每个主机就可以申请7个executor，每个executor可以运行4个任务，每个core一个task。那么每个task的平均内存是 14/4 = 3.5GB。在executor运行的task共享内存。
其实，executor内部是用`newCachedThreadPool`运行task的。

确保 `spark.executor.memoryOverhead `和 `spark.executor.memory`的和不超过`yarn.scheduler.maximum-allocation-mb`

2. driver内存

对于drvier的内存配置，当然也包括两个参数。

* spark.driver.memoryOverhead	每个driver能从yarn申请的堆外内存的大小。

* spark.driver.memory 当运行hive on spark的时候，每个spark driver能申请的最大jvm 堆内存。该参数结合 `spark.driver.memoryOverhead`共同决定着driver的内存大小。

driver的内存大小并不直接影响性能，但是也不要job的运行受限于driver的内存. 这里给出spark driver内存申请的方案，假设`yarn.nodemanager.resource.memory-mb`是 X。

* driver内存申请12GB，假设 X > 50GB
* driver内存申请 4GB，假设 12GB < X <50GB
* driver内存申请1GB,假设 1GB < X < 12 GB
* driver内存申请256MB，假设 X < 1GB

这些数值是 `spark.driver.memory `和 `spark.driver.memoryOverhead`内存的总和。对外内存站总内存的10%-15%。
假设 `yarn.nodemanager.resource.memory-mb=100*1024`MB,那么driver内存设置为12GB，此时
`spark.driver.memory=10.5gb `和` spark.driver.memoryOverhead=1.5gb`

注意，资源多少直接对应的是数据量的大小。


3. executor数

executor的数目是由每个节点运行的executor数目和集群的节点数共同决定。如果你有四十个节点，那么hive可以使用的最大executor数就是 280(40*7).
最大数目可能比这个小点，因为driver也会消耗1core和12GB。 

当前假设是没有yarn应用在跑。

Hive性能与用于运行查询的executor数量直接相关。 但是，不通查询还是不通。 通常，性能与executor的数量成比例。 例如，查询使用四个executor大约需要使用两个executor的一半时间。
但是，性能在一定数量的executor中达到峰值，高于此值时，增加数量不会改善性能并且可能产生不利影响。

在大多数情况下，使用一半的集群容量（executor数量的一半）可以提供良好的性能。 为了获得最佳性能，最好使用所有可用的executor。
例如，设置`spark.executor.instances = 280 `。 对于基准测试和性能测量，强烈建议这样做。

4. 动态executor申请

虽然将`spark.executor.instances`设置为最大值通常可以最大限度地提高性能，但不建议在多个用户运行Hive查询的生产环境中这样做。
避免为用户会话分配固定数量的executor，因为如果executor空闲，executor不能被其他用户查询使用。 在生产环境中，应该好好计划executor分配，以允许更多的资源共享。

Spark允许您根据工作负载动态扩展分配给Spark应用程序的集群资源集。 要启用动态分配，请按照动态分配中的步骤进行操作。 除了在某些情况下，强烈建议启用动态分配。

5. 并行度

要使可用的executor得到充分利用，必须同时运行足够的任务（并行）。在大多数情况下，Hive会自动确定并行度，但也可以在调优并发度方面有一些控制权。
在输入端，map任务的数量等于输入格式生成的split数。对于Hive on Spark，输入格式为CombineHiveInputFormat，它可以根据需要对基础输入格式生成的split进行分组。
可以更好地控制stage边界的并行度。调整hive.exec.reducers.bytes.per.reducer以控制每个reducer处理的数据量，Hive根据可用的executor，执行程序内存，以及其他因素来确定最佳分区数。
实验表明，只要生成足够的任务来保持所有可用的executor繁忙，Spark就比MapReduce对`hive.exec.reducers.bytes.per.reducer`指定的值敏感度低。
为获得最佳性能，请为该属性选择一个值，以便Hive生成足够的任务以完全使用所有可用的executor。

## hive配置

Hive on spark 共享了很多hive性能相关的配置。可以像调优hive on mapreduce一样调优hive on spark。
然而，`hive.auto.convert.join.noconditionaltask.size`是基于统计信息将基础join转化为map join的阈值，可能会对性能产生重大影响。
尽管该配置可以用hive on mr和hive on spark，但是两者的解释不同。

数据的大小有两个统计指标：
* totalSize- 数据在磁盘上的近似大小。
* rawDataSize- 数据在内存中的近似大小。

hive on mr用的是totalSize。hive on spark使用的是rawDataSize。由于可能存在压缩和序列化，这两个值会有较大的差别。
对于hive on spark 需要将 `hive.auto.convert.join.noconditionaltask.size`指定为更大的值，才能将与hive on mr相同的join转化为map join。

可以增加此参数的值，以使地图连接转换更具凶猛。 将common join 转换为 map join 可以提高性能。 
如果此值设置得太大，则来自小表的数据将使用过多内存，任务可能会因内存不足而失败。 根据群集环境调整此值。

通过参数 `hive.stats.collect.rawdatasize` 可以控制是否收集 rawDataSize 统计信息。

对于hiveserver2，建议再配置两个额外的参数: `hive.stats.fetch.column.stats=true` 和 `hive.optimize.index.filter=true`.

Hive性能调优通常建议使用以下属性：

```
hive.optimize.reducededuplication.min.reducer=4
hive.optimize.reducededuplication=true
hive.merge.mapfiles=true
hive.merge.mapredfiles=false
hive.merge.smallfiles.avgsize=16000000
hive.merge.size.per.task=256000000
hive.merge.sparkfiles=true
hive.auto.convert.join=true
hive.auto.convert.join.noconditionaltask=true
hive.auto.convert.join.noconditionaltask.size=20M(might need to increase for Spark, 200M)
hive.optimize.bucketmapjoin.sortedmerge=false
hive.map.aggr.hash.percentmemory=0.5
hive.map.aggr=true
hive.optimize.sort.dynamic.partition=false
hive.stats.autogather=true
hive.stats.fetch.column.stats=true
hive.compute.query.using.stats=true
hive.limit.pushdown.memory.usage=0.4 (MR and Spark)
hive.optimize.index.filter=true
hive.exec.reducers.bytes.per.reducer=67108864
hive.smbjoin.cache.rows=10000
hive.fetch.task.conversion=more
hive.fetch.task.conversion.threshold=1073741824
hive.optimize.ppd=true
```


## 预热YARN容器

在开始新会话后提交第一个查询时，在查看查询开始之前可能会遇到稍长的延迟。还会注意到，如果再次运行相同的查询，它的完成速度比第一个快得多。

Spark执行程序需要额外的时间来启动和初始化yarn上的Spark，这会导致较长的延迟。此外，Spark不会等待所有executor在启动作业之前全部启动完成，因此在将作业提交到群集后，某些executor可能仍在启动。
但是，对于在Spark上运行的作业，作业提交时可用executor的数量部分决定了reducer的数量。当就绪executor的数量未达到最大值时，作业可能没有最大并行度。这可能会进一步影响第一个查询的性能。

在用户较长期会话中，这个额外时间不会导致任何问题，因为它只在第一次查询执行时发生。然而，诸如Oozie发起的Hive工作之类的短期绘画可能无法实现最佳性能。

为减少启动时间，可以在作业开始前启用容器预热。只有在请求的executor准备就绪时，作业才会开始运行。这样，在reduce那一侧不会减少短会话的并行性。

要启用预热功能，请在发出查询之前将`hive.prewarm.enabled`设置为true。还可以通过设置`hive.prewarm.numcontainers`来设置容器数量。默认值为10。

预热的executor的实际数量受`spark.executor.instances`（静态分配）或`spark.dynamicAllocation.maxExecutors`（动态分配）的值限制。 `hive.prewarm.numcontainers`的值不应超过分配给用户会话的值。

注意：预热需要几秒钟，对于短会话来说是一个很好的做法，特别是如果查询涉及reduce阶段。 但是，如果`hive.prewarm.numcontainers`的值高于群集中可用的值，则该过程最多可能需要30秒。 请谨慎使用预热。