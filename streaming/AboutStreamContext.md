
## 1. start做了啥
```scala
 /**
   * Start the execution of the streams.
   *
   * @throws IllegalStateException if the StreamingContext is already stopped.
   */
  def start(): Unit = synchronized {
    state match {
      case INITIALIZED =>
        startSite.set(DStream.getCreationSite())
        StreamingContext.ACTIVATION_LOCK.synchronized {
          StreamingContext.assertNoOtherContextIsActive()
          try {
            validate()

            // Start the streaming scheduler in a new thread, so that thread local properties
            // like call sites and job groups can be reset without affecting those of the
            // current thread.
//            在新线程中启动streaming scheduler，以便线程本地属性可以重新设置而不影响当前线程。例如call sites 和job groups

            ThreadUtils.runInNewThread("streaming-start") {
              sparkContext.setCallSite(startSite.get)
              sparkContext.clearJobGroup()
              sparkContext.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
              savedProperties.set(SerializationUtils.clone(sparkContext.localProperties.get()))

//              启动JobScheduler
              scheduler.start()
            }


            state = StreamingContextState.ACTIVE

            scheduler.listenerBus.post(
              StreamingListenerStreamingStarted(System.currentTimeMillis()))
          } catch {
            case NonFatal(e) =>
              logError("Error starting the context, marking it as stopped", e)
              scheduler.stop(false)
              state = StreamingContextState.STOPPED
              throw e
          }
          StreamingContext.setActiveContext(this)
        }
        logDebug("Adding shutdown hook") // force eager creation of logger
        shutdownHookRef = ShutdownHookManager.addShutdownHook(
          StreamingContext.SHUTDOWN_HOOK_PRIORITY)(() => stopOnShutdown())
        // Registering Streaming Metrics at the start of the StreamingContext
        assert(env.metricsSystem != null)
//        注册度量系统-StreamingSource
        env.metricsSystem.registerSource(streamingSource)

//        绑定ui
        uiTab.foreach(_.attach())
        logInfo("StreamingContext started")
      case ACTIVE =>
        logWarning("StreamingContext has already been started")
      case STOPPED =>
        throw new IllegalStateException("StreamingContext has already been stopped")
    }
  }
```
代码重点分几个块：
1，JobScheduler的启动，关于JobScheduler后面会详细说到
2，初始化StreamGraph。
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
3，新线程的作用
```scala
//  在新线程中启动streaming scheduler，以便线程本地属性可以重新设置而不影响当前线程。例如call sites 和job groups

    ThreadUtils.runInNewThread("streaming-start") {
      sparkContext.setCallSite(startSite.get)
      sparkContext.clearJobGroup()
      sparkContext.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
      savedProperties.set(SerializationUtils.clone(sparkContext.localProperties.get()))

//    启动JobScheduler
      scheduler.start()
    }
```
4，度量系统，注册，我们也可以自定义度量系统，使用接口注册。
```scala
//        注册度量系统-StreamingSource
        env.metricsSystem.registerSource(streamingSource)
```
5，StreamingListener注册。预留的接口如下：
```scala
//  添加Streaminglistener ，可以接收该streaming相关的系统事件
  def addStreamingListener(streamingListener: StreamingListener) {
    scheduler.listenerBus.addListener(streamingListener)
  }
```


