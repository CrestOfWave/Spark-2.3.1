
1. 资源概述

单个executor在standalone模式下，资源申请的参数是:
spark.executor.cores(spark-defaults.conf) == --executor-cores(shell)
spark.executor.memory(spark-defaults.conf) == --executor-memory(shell) 

那么，我们在standalone模式下是没有看到如何设置，executor总数的。那么，如何控制executor总数的呢？

实际上这里还有一个参数，--total-executor-cores ，该值处以单个executor所占用的cpu数，就是executor数。


2. 调度策略

调度策略在standalone模式下有两种策略：
1) 尽可能的将executor调度到较少的workers

2) 尽可能的将executor均匀调度到workers上

第一种能更好的利用数据本地性，所以是默认的方式。当然，我们可以通过将 spark.deploy.spreadOut 设置为false来选择第二种。

3. 疑问点

1)
--total-executor-cores申请的总cpu数大于 所有worker的cpu总数，该情况下怎么办呢？
实际上代码里做了判断，取二者值最小的那个

```scala
var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
```

2)
还有一点要强调的是 spark.executor.cores 该参数在standalone模式下是未设置的,
这种情况下会单个executor会尽可能多的吃掉单个worker的cpu资源，有可能会导致一个app就会仅在一个worker且仅一个excutor。

详细可看代码。

假如，该 spark.executor.cores 参数设置了 ，那么单个executor申请的cpu数就是确定的，而不是尽可能多的吃掉worker的资源。

4. 调度的代码实现

````scala
private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
//          判断是不是一个worker仅仅加载一个executor，由是否配置spark.executor.cores决定，假如配置了，oneExecutorPerWorker值就为false。
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.

//          spark.deploy.spreadOut 参数值
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }
````
