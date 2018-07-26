
### 1. backpressure-背压

backpressure后面一律叫做背压。消费者消费的速度低于生产者生产的速度，为了使应用正常，消费者会反馈给生产者来调节生产者生产的速度，以使得消费者需要多少，生产者生产多少。


### 2. Spark Streaming的backpressure

阅读本文前，需要掌握：

[1. PID控制器](https://blog.csdn.net/qq1205512384/article/details/72614871)

2. StreamingListener

Spark Streaming 跟kafka结合是存在背压机制的。目标是根据当前job的处理情况，来调节后续批次的获取kafka消息的条数。
为了达到这个目的Spark Streaming在原有的架构上加入了一个RateController，利用的算法是PID，需要的反馈数据是任务处理的结束时间，调度时间，处理时间，消息条数，这些数据是通过StreamingListener体系获得，然后通过PIDRateEsimator的compute计算得到一个速率，进而可以计算得到一个offset，然后跟你的限速设置最大消费条数做比较得到一个最终要消费的消息最大offset。
当然也要限制计算的最大offset小于kafka分区的最大offset。

### 3. 背压源码赏析

背压源码赏析我们采用的源码是Spark Streaming 与 kafka 0.10版本的结合。

#### 3.1 代码入口
入口当然还是`DirectKafkaInputDStream.compute`,在其第一行，计算当前要生成的KafkaRDD最大截止offset。

```scala
//    获取当前生成job，要用到的KafkaRDD每个分区最大消费偏移值
    val untilOffsets = clamp(latestOffsets())
```

#### 3.2 获取kafka 分区最大offset

求kafka 分区最大offset过程是在latestOffsets()方法中。该方法有两个目的：

1. 获取新增分区，并使其在本次生效。

2. 获取所有分区在kafka中的最大offset。

```scala
 /**
   * Returns the latest (highest) available offsets, taking new partitions into account.
    *  返回最大可提供offset，并将新增分区生效
   */
  protected def latestOffsets(): Map[TopicPartition, Long] = {
    val c = consumer
    paranoidPoll(c)
    // 获取所有的分区信息
    val parts = c.assignment().asScala

    // make sure new partitions are reflected in currentOffsets
    // 做差获取新增的分区信息
    val newPartitions = parts.diff(currentOffsets.keySet)
    // position for new partitions determined by auto.offset.reset if no commit
    // 新分区消费位置，没有记录的化是由auto.offset.reset决定
    currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap
    // don't want to consume messages, so pause
    // 不会在这里获取数据，所以要pause
    c.pause(newPartitions.asJava)
    // find latest available offsets
//    找到kafka可提供的最大offsets
    c.seekToEnd(currentOffsets.keySet.asJava)
    parts.map(tp => tp -> c.position(tp)).toMap
  }
```

#### 3.3 限制不超kafka 分区的最大offset-clamp

由于，该过程是个嵌套的过程所以，我们在这里是从外层往内层穿透。

限制不超kafka 分区的最大offset实现方法是clamp，需要的参数是通过`latestOffsets`获取的kafka所有分区的最大offset。具体实现代码如下：

```scala
  // limits the maximum number of messages per partition
//  限制每个分区处理的最大消息条数不超过kafka 分区里的最大offset
  protected def clamp(
    offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {

    maxMessagesPerPartition(offsets).map { mmp =>
      mmp.map { case (tp, messages) =>
          val uo = offsets(tp) // kafka的最大偏移
          tp -> Math.min(currentOffsets(tp) + messages, uo) // 限制最大偏移应该小于等于kafka最大offset
      }
    }.getOrElse(offsets)
  }
```

这里面主要是将通过maxMessagesPerPartition计算出来的offset和kafka 分区最大offset做比较，限制当前要消费的最大offset不超过kafka可提供的offset。

#### 3.4 计算最大offset-maxMessagesPerPartition

计算每个分区的要消费的最大offset总共有以下几个步骤：

##### 1. 计算最大 rate msg/s
第一种情况，PID计算器计算的Rate值存在

    1. 求出每个分区滞后offset：lagPerPartition
    2. 求出总的lag：totalLag
    3. 求出背压的速率：backpressureRate = Math.round(lag / totalLag.toFloat * rate)
    4. 假如设置了 spark.streaming.kafka.maxRatePerPartition > 0,计算的最大rate不能超过该值。未设置，也即是spark.streaming.kafka.maxRatePerPartition<=0 ,则返回的就是背压速率backpressureRate
    5. 将batch时间转化为s，并且乘以前面算的速率，得到最大oofset。

第二种情况，PID计算器计算的Rate值不存在

    返回值就是每个分区获取通过 spark.streaming.kafka.maxRatePerPartition 参数设置的最大rate。

##### 2. 计算得到最大offset

该值要区别于kafka 分区的最大offset，这个最大offset是通过计算出来的rate，乘以batch time得到的，可能会超过kafka 分区内最大的offset。


```scala
  protected[streaming] def maxMessagesPerPartition(
    offsets: Map[TopicPartition, Long]): Option[Map[TopicPartition, Long]] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate())

    // calculate a per-partition rate limit based on current lag
//    基于当前lag，来计算每个分区的速率。
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
//        求出每个分区滞后的消息offset
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        val totalLag = lagPerPartition.values.sum

        lagPerPartition.map { case (tp, lag) =>
//          取出分区配置的最大限速 速率，由参数 spark.streaming.kafka.maxRatePerPartition 配置
          val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)
//          计算背压rate
          val backpressureRate = Math.round(lag / totalLag.toFloat * rate)

//          计算每个分区要消费的最大offset，假如配置了spark.streaming.kafka.maxRatePerPartition，就取背压速率和最大速率的最小值，假如没有最大限速，就取背压速率

          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)} else backpressureRate)
        }
//       假如PID计算器没有计算出大于0的速率，或者没有使用(新增分区)，那么就采用配置中的最大限速速率
      case None => offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp) }
    }

//    将batch时间转化为s，并且乘以前面算的速率，得到最大oofset。
    if (effectiveRateLimitPerPartition.values.sum > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        case (tp, limit) => tp -> (secsPerBatch * limit).toLong
      })
    } else {
      None
    }
  }
```

#### 3.5 PID算法计算出 rate的过程

rate 是针对所有分区的，不是每个分区一个rate。

rate 计算是通过pid算法，需要通过StreamingListener体系获取以下四个数据指标：

1. 处理结束时间 batchCompleted.batchInfo.processingEndTime
2. 处理时间 batchCompleted.batchInfo.processingDelay
3. 调度延迟 batchCompleted.batchInfo.schedulingDelay
4. 消息条数 elements.get(streamUID).map(_.numRecords)

针对pid算法的计算肯定简单来说主要由以下几个步骤：

1. 计算偏差

`val error = latestRate - processingRate`

2. 计算积分

`val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis`
积分的思路，调度延迟*处理速率得到总的未处理消息数，再除以批处理时间得到的值，可以视为就是历史累计速率，也即是积分部分。

这里假设处理速率变化不大。

3. 计算微分

`val dError = (error - latestError) / delaySinceUpdate`

4. 计算新的速率

`val newRate = (latestRate - proportional * error -
                                     integral * historicalError -
                                     derivative * dError).max(minRate)`

5. 更新参数
```scala
//        更新参数
        latestTime = time
        if (firstRun) {
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          None
        } else {
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(newRate)
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
```

具体计算过程在PIDRateEstimator的compute方法。
```scala
 def compute(
      time: Long, // in milliseconds processingEnd
      numElements: Long, // numRecords
      processingDelay: Long, // in milliseconds processingDelay
      schedulingDelay: Long // in milliseconds schedulingDelay
    ): Option[Double] = {
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {

//        以秒为单位，改时间应该接近于batchtime
        val delaySinceUpdate = (time - latestTime).toDouble / 1000

//        每秒钟处理的消息条数
        val processingRate = numElements.toDouble / processingDelay * 1000

//        error就是目标rate和当前测量rate的差值。其中，目标rate是该estimator为上个批次计算的rate
//        单位是 elements/second
//        该值就是PID中的P系数要乘以的值
        val error = latestRate - processingRate

//        该值应该是历史Error的积分。
//        调度延迟乘以处理速率，得到的就是历史积压的未处理的元素个数。因为是有这些为处理元素的挤压才导致的有这么长的调度延迟。当然，这里是假设处理速率变化不大。
//        得到未处理元素个数，除以批处理时间，就是当前需要处理元素的速率。这个既可以当成历史Error的积分
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis

        // in elements/(second ^ 2)
        val dError = (error - latestError) / delaySinceUpdate

//        计算得到新的速率
        val newRate = (latestRate - proportional * error -
                                    integral * historicalError -
                                    derivative * dError).max(minRate)
        logTrace(s"""
            | latestRate = $latestRate, error = $error
            | latestError = $latestError, historicalError = $historicalError
            | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)

//        更新参数
        latestTime = time
        if (firstRun) {
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          None
        } else {
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(newRate)
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
    }
  }
```
PID控制器参数的配置

```scala
 def create(conf: SparkConf, batchInterval: Duration): RateEstimator =
    conf.get("spark.streaming.backpressure.rateEstimator", "pid") match {
      case "pid" =>
        val proportional = conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
        val integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
        val derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
        val minRate = conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
        new PIDRateEstimator(batchInterval.milliseconds, proportional, integral, derived, minRate)

      case estimator =>
        throw new IllegalArgumentException(s"Unknown rate estimator: $estimator")
    }
```

#### 3.6 数据获取

pid计算器是需要数据采样输入的，这里获取的数据的方式是StreamingListener体系。

DirectKafkaInputDStream里有个叫做DirectKafkaRateController类。

```scala
  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }
```

每个批次执行结束之后，会调用RateController的
```scala
override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val elements = batchCompleted.batchInfo.streamIdToInputInfo

    for {
      processingEnd <- batchCompleted.batchInfo.processingEndTime
      workDelay <- batchCompleted.batchInfo.processingDelay
      waitDelay <- batchCompleted.batchInfo.schedulingDelay
      elems <- elements.get(streamUID).map(_.numRecords)
    } computeAndPublish(processingEnd, elems, workDelay, waitDelay)
  }
```

在computeAndPublish方法中，调用了我们的PIDRateEstimator.compute

```scala
/**
   * Compute the new rate limit and publish it asynchronously.
   */
  private def computeAndPublish(time: Long, elems: Long, workDelay: Long, waitDelay: Long): Unit =
    Future[Unit] {
      val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
      newRate.foreach { s =>
        rateLimit.set(s.toLong)
        publish(getLatestRate())
      }
    }
```

StreamingListener加入StreamingListenerBus是在JobScheduler类的start方法中

```scala
  // attach rate controllers of input streams to receive batch completion updates
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController)

    listenerBus.start()
```

源码是很精彩的，阅读越多越痴迷。

欢迎关注浪尖公众号：
![image](../微信公众号.jpg)
或者加入知识星球
![image](../知识星球.jpg)