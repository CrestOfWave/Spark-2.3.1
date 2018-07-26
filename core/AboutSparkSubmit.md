
## 1. 书写本文的目的
前面，已经有文章说过如何配置一个spark-submit脚本及相关参数的解析。没有阅读过的可以参考下面文章阅读。

[Sparksubmit提交应用脚本编写](https://mp.weixin.qq.com/s/l9BIZnZKNS60XFDZi3PQsw)

[Sparksubmit参数解析那些事](https://mp.weixin.qq.com/s/mKxQtmTmE59Q4DsDjkicuA)

本文设计的目的是什么呢？
>在浪尖的微信群里，有人曾经问过这样的问题。spark-submit命令提交应用的时候没有指定部署模式，而是在代码里写了部署模式。当然，这样做出了bug，经过浪尖的指点将部署模式编写到了spark-submit脚本里面，这样允许满足需求。

都说配置的优先级应该是：代码里配置 > spark-submit脚本配置 > 配置文件spark-defaults.conf配置。
为啥master不行呢？
浪尖在这里总结一下，cluster模式，由于sparksubmit端并没进入用户代码，这个时候在用户代码里设置的配置肯定都没有使用生效。所以，没满足上面所说的优先级。

下面进入代码验证阶段。

## 2. spark-submit脚本执行解读

首先，我们先去打开该脚本，可以看到，有如下一段代码：
```shell
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

disable randomized hash for string in Python 3.3+
export PYTHONHASHSEED=0

SparkSubmit是利用Spark-submit提交应用程序的入口类。进入SparkSubmit的main函数。
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```

这里浪尖就不带着大家深入挖掘spark-class，其实他是拼凑了一个命令执行了,下面的main方法。

```scala
org.apache.spark.launcher.Main
```

最终，还是执行了SparkSubmit的main方法。直接进入sparksubmit的main方法。
```scala
  override def main(args: Array[String]): Unit = {
    // Initialize logging if it hasn't been done yet. Keep track of whether logging needs to
    // be reset before the application starts.
    val uninitLog = initializeLogIfNecessary(true, silent = true)

//    解析参数
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
  }
```
我们这里分两个步骤解析：

1，参数解析

2，命令提交

### 参数解析-SparkSubmitArguments

进入之后，可以很轻松发现构建SparkSubmitArguments的时候就会执行的代码片段。
```scala
  // Set parameters from command line arguments
//  从命令行参数获取解析参数
  try {
    parse(args.asJava)
  } catch {
    case e: IllegalArgumentException =>
      SparkSubmit.printErrorAndExit(e.getMessage())
  }
  // Populate `sparkProperties` map from properties file
//  从属性配置文件中填充 sparkProperties 这个map
  mergeDefaultSparkProperties()
  // Remove keys that don't start with "spark." from `sparkProperties`.
  ignoreNonSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters
  loadEnvironmentArguments()

  validateArguments()
```

#### 1。parse函数

该函数主要是负责解析spark-submit命令行参数。主要分成两个部分：

1，spark运行环境参数解析,代表方法是SparkSubmitArguments$handle(opt: String, value: String)，不同的部署模式稍微有些区别。
在这里做一点说明，--conf的配置被写道了sparkProperties这个hashmap里了。

2，用户类参数解析，代表方法是SparkSubmitArguments$handleExtraArgs(args.subList(idx, args.size()));这个会传给用户类。

```scala
 /**
   * Parse a list of spark-submit command line options.
   * <p>
   * See SparkSubmitArguments.scala for a more formal description of available options.
   * 解析spark-submit命令行参数
   * @throws IllegalArgumentException If an error is found during parsing.
   */
  protected final void parse(List<String> args) {
    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    int idx = 0;
    for (idx = 0; idx < args.size(); idx++) {
      String arg = args.get(idx);
      String value = null;

      Matcher m = eqSeparatedOpt.matcher(arg);
      if (m.matches()) {
        arg = m.group(1);
        value = m.group(2);
      }

      // Look for options with a value.
      String name = findCliOption(arg, opts);
      if (name != null) {
        if (value == null) {
          if (idx == args.size() - 1) {
            throw new IllegalArgumentException(
                String.format("Missing argument for option '%s'.", arg));
          }
          idx++;
          value = args.get(idx);
        }
        if (!handle(name, value)) {
          break;
        }
        continue;
      }

      // Look for a switch.
      name = findCliOption(arg, switches);
      if (name != null) {
        if (!handle(name, null)) {
          break;
        }
        continue;
      }

      if (!handleUnknown(arg)) {
        break;
      }
    }

    if (idx < args.size()) {
      idx++;
    }
//    之外的参数会当成，用户入口类的参数
    handleExtraArgs(args.subList(idx, args.size()));
  }
```

#### 2。 mergeDefaultSparkProperties()
合并配置文件里的默认配置属性和--conf指定的配置属性，在这里可以看书--conf优先级更高.
````scala
/**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
    * 合并配置文件里的默认配置属性和--conf指定的配置属性，在这里可以看书--conf优先级更高，  
   */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
//    如果用户没有指定默认属性配置文件，将使用公用的属性配置文件
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }
````
个人觉得上面的代码片段还是需要解析一下的，首先是，获取属性配置文件：
```scala
 propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
 
 这里可以拆开getDefaultPropertiesFile来看，配置文件的路径
 
 /** Return the path of the default Spark properties file. */
   def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
     env.get("SPARK_CONF_DIR")
       .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
       .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
       .filter(_.isFile)
       .map(_.getAbsolutePath)
       .orNull
   }
```

获取完属性配置文件之后，可能会感觉莫名其秒。

为啥没见使用呢？说好的合并呢？

实际上，这里有个scala的编程语言特点，lazy形式的变量-defaultSparkProperties。

````scala
  /** Default properties present in the currently defined defaults file. */
//    当前配置文件里的默认属性配置，会在mergeDefaultSparkProperties赋值给sparkProperties
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    // scalastyle:off println
    if (verbose) SparkSubmit.printStream.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      val properties = Utils.getPropertiesFromFile(filename)
      properties.foreach { case (k, v) =>
        defaultProperties(k) = v
      }
      // Property files may contain sensitive information, so redact before printing
      if (verbose) {
        Utils.redact(properties).foreach { case (k, v) =>
          SparkSubmit.printStream.println(s"Adding default property: $k=$v")
        }
      }
    }
    // scalastyle:on println
    defaultProperties
  }
````
可以看到这段代码里，是将配置文件的属性读取，然后添加到了defaultProperties。最终是返回作为defaultSparkProperties，该变量会在调用的地方执行初始化，然后在调用的foreach方法中将
属性书写到sparkProperties。这里并没有覆盖掉命令行参数解析里获取的配置。

从而保证了命令行的参数优先级大于默认配置文件。

#### 3。ignoreNonSparkProperties()

主要是移除调不是spark.开头的配置

```scala
/**
   * Remove keys that don't start with "spark." from `sparkProperties`.
   */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.foreach { case (k, v) =>
      if (!k.startsWith("spark.")) {
        sparkProperties -= k
        SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
      }
    }
  }
```

#### 4。填充必须参数-loadEnvironmentArguments

主要是用上面获取到的sparkProperties和env里面的变量来填充缺失的变量。
其中就包括master，这也告诉我们，这些参数除了可以在配置文件或者sparksubmit命令行里面进行配置外，也可以配置系统环境变量。
```scala
/**
   * Load arguments from environment variables, Spark properties etc.
    *  从系统环境变量和spark properties中加载参数。
   */
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get("spark.driver.cores"))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get("spark.executor.memory"))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get("spark.executor.cores"))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get("spark.cores.max"))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get("spark.jars")).orNull
    files = Option(files).orElse(sparkProperties.get("spark.files")).orNull
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull
    ivySettingsPath = sparkProperties.get("spark.jars.ivySettings")
    packages = Option(packages).orElse(sparkProperties.get("spark.jars.packages")).orNull
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get("spark.jars.excludes")).orNull
    repositories = Option(repositories)
      .orElse(sparkProperties.get("spark.jars.repositories")).orNull
    deployMode = Option(deployMode)
      .orElse(sparkProperties.get("spark.submit.deployMode"))
      .orElse(env.get("DEPLOY_MODE"))
      .orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get("spark.executor.instances").orNull)
    queue = Option(queue).orElse(sparkProperties.get("spark.yarn.queue")).orNull
    keytab = Option(keytab).orElse(sparkProperties.get("spark.yarn.keytab")).orNull
    principal = Option(principal).orElse(sparkProperties.get("spark.yarn.principal")).orNull

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            Utils.tryWithResource(new JarFile(uri.getPath)) { jar =>
              // Note that this might still return null if no main-class is set; we catch that later
              mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
            }
          } catch {
            case _: Exception =>
              SparkSubmit.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          SparkSubmit.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
            "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local[*]")

    // In YARN mode, app name can be set via SPARK_YARN_APP_NAME (see SPARK-5222)
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }

    // Action should be SUBMIT unless otherwise specified
    action = Option(action).getOrElse(SUBMIT)
  }
```

#### 5。验证参数-validateArguments()

该方法的主要目的是验证必须的参数是否存在。
```scala
  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
//  仅仅确保必要的参数是必须要存在的
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
    }
  }
```

对于提交阶段。
````scala
  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python or R file)")
    }
    if (mainClass == null && SparkSubmit.isUserJar(primaryResource)) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (driverMemory != null
        && Try(JavaUtils.byteStringAsBytes(driverMemory)).getOrElse(-1L) <= 0) {
      SparkSubmit.printErrorAndExit("Driver Memory must be a positive number")
    }
    if (executorMemory != null
        && Try(JavaUtils.byteStringAsBytes(executorMemory)).getOrElse(-1L) <= 0) {
      SparkSubmit.printErrorAndExit("Executor Memory cores must be a positive number")
    }
    if (executorCores != null && Try(executorCores.toInt).getOrElse(-1) <= 0) {
      SparkSubmit.printErrorAndExit("Executor cores must be a positive number")
    }
    if (totalExecutorCores != null && Try(totalExecutorCores.toInt).getOrElse(-1) <= 0) {
      SparkSubmit.printErrorAndExit("Total executor cores must be a positive number")
    }
    if (numExecutors != null && Try(numExecutors.toInt).getOrElse(-1) <= 0) {
      SparkSubmit.printErrorAndExit("Number of executors must be a positive number")
    }
    if (pyFiles != null && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        throw new Exception(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }

    if (proxyUser != null && principal != null) {
      SparkSubmit.printErrorAndExit("Only one of --proxy-user or --principal can be provided.")
    }
  }
````

### 任务提交

任务提交主要的入口是submit(), 该过程分两步：
第一步，准备加载环境，主要获取配置的途径有何时的系统路径，系统属性参数，应用程序参数（目的是根据集群管理器和部署模式运行 childmain类）。

第二步，使用加载的环境去激活child main class

首先，我们先看第一步：
```scala
val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)

private[deploy] def prepareSubmitEnvironment(
      args: SparkSubmitArguments,
      conf: Option[HadoopConfiguration] = None)
      : (Seq[String], Seq[String], SparkConf, String) = {
    try {
      doPrepareSubmitEnvironment(args, conf)
    } catch {
      case e: SparkException =>
        printErrorAndExit(e.getMessage)
        throw e
    }
  }
```
由于该方法太过冗长，在这里不贴源码了，请直接进入到SparkSubmit.scala的该方法里看注释。
代码里很容易会发现，在进入用户代码之前已经确定了部署模式，并且按照部署模式执行了相应的childmain。这个时候有几种情况可以测试：

1,spark-submit指定yarn-client或者yarn-cluster模式下，在用户代码里setMaster设置为了local[*]。

2,spark-submit指定为local模式下，在用户代码里setmaster设置为master或者client。

3,在spark-submit指定为client或者local的模式下，用户代码里设置master为cluster。

在这几种情况下，SparkContext在创建createTaskScheduler方法里会根据最终设置的配置来创建调度模式，由于不同的模式在SparkSubmit解析的时候生成参数和依赖的方式不一样，会有相应的错误。
这里对比和错误展示，后面抽空浪尖整理给出。
