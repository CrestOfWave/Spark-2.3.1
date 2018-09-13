```scala
package bigdata.spark.SparkSQL.DatasetApi

import java.sql.Date
import java.time.{LocalDate, Month}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc_nulls_last, desc_nulls_last, grouping_id, lit, month, sum, year,col,struct,expr}

object MultiDimensionalAggregation {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances","2")
      .set("spark.default.parallelism","4")
      .set("spark.sql.shuffle.partitions","4")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
        ,"/opt/jars/kafka-clients-0.10.2.2.jar"
        ,"/opt/jars/kafka_2.11-0.10.2.2.jar"))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // groupby groupsets rullup
//    rullupAsGroupByTest(spark)


    // rullup test
//    rullupTest(spark)

    // groupby as roll up
//    groupByAsRullupTest(spark)

    // cubeTest
//    cubeTest(spark)

//    groupingsetsTest(spark)

    PivotTest(spark)
  }
  def groupingsetsTest(spark:SparkSession)={
    val sales = spark.createDataFrame(Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    )).toDF("city", "year", "amount")

    sales.createOrReplaceTempView("sales")

    // 等价于 rollup("city", "year")
    spark.sql("""
      SELECT city, year, sum(amount) as amount
      FROM sales
      GROUP BY city, year
      GROUPING SETS ((city, year), (city), ())
      ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
    """)
      .show()

    // 等价于 cube("city", "year")
    spark.sql("""
        SELECT city, year, sum(amount) as amount
        FROM sales
        GROUP BY city, year
        GROUPING SETS ((city, year), (city), (year), ())
        ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
    """)
      .show()
  }
  def PivotTest(spark:SparkSession)={
    val sales = spark.createDataFrame(Seq(
      ("Warsaw", 2016, 100,"Warsaw"),
      ("Warsaw", 2017, 200,"Warsaw"),
      ("Warsaw", 2016, 100,"Warsaw"),
      ("Warsaw", 2017, 200,"Warsaw"),
      ("Boston", 2015, 50,"Boston"),
      ("Boston", 2016, 150,"Boston"),
      ("Toronto", 2017, 50,"Toronto")
    )).toDF("city", "year", "amount","test")
    sales
      .groupBy("year").pivot("city",Seq("Warsaw","Boston","Toronto"))
      .agg(sum("amount") as "amount")
      .show()
  }
  def cubeTest(spark:SparkSession)={
    val sales = spark.createDataFrame(Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    )).toDF("city", "year", "amount")

    sales.cube("city", "year")
      .agg(sum("amount") as "amount")
      .sort(col("city").desc_nulls_last, col("year").asc_nulls_last)
      .show()
  }
  def groupByAsRullupTest(spark:SparkSession)={
    val sales = spark.createDataFrame(Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    )).toDF("city", "year", "amount")

    sales
      .rollup("city", "year")
      .agg(sum("amount") as "amount")
      .sort(desc_nulls_last("city"), asc_nulls_last("year"))
      .show()

    // The above query is semantically equivalent to the following
    val q1 = sales
      .groupBy("city", "year")  // <-- subtotals (city, year)
      .agg(sum("amount") as "amount")
    val q2 = sales
      .groupBy("city")          // <-- subtotals (city)
      .agg(sum("amount") as "amount")
      .select(col("city"), lit(null) as "year", col("amount"))  // <-- year is null
    val q3 = sales
      .groupBy()                // <-- grand total
      .agg(sum("amount") as "amount")
      .select(lit(null) as "city", lit(null) as "year", col("amount"))  // <-- city and year are null
    q1.union(q2)
      .union(q3)
      .sort(desc_nulls_last("city"), asc_nulls_last("year"))
      .show()
  }
  def rollupTestinventory(spark:SparkSession)={
    // Borrowed from Microsoft's "Summarizing Data Using ROLLUP" article
    val inventory = spark.createDataFrame(Seq(
      ("table", "blue", 124),
      ("table", "red", 223),
      ("chair", "blue", 101),
      ("chair", "red", 210))).toDF("item", "color", "quantity")
    inventory.show()

    inventory.rollup("item", "color").
      sum().
//      sort(col("year").asc_nulls_last, col("month").asc_nulls_last).
      show
//    // Borrowed from http://stackoverflow.com/a/27222655/1305344
//    val quarterlyScores = spark.createDataFrame(Seq(
//      ("winter2014", "Agata", 99),
//      ("winter2014", "Jacek", 97),
//      ("summer2015", "Agata", 100),
//      ("summer2015", "Jacek", 63),
//      ("winter2015", "Agata", 97),
//      ("winter2015", "Jacek", 55),
//      ("summer2016", "Agata", 98),
//      ("summer2016", "Jacek", 97))).toDF("period", "student", "score")
//
//    quarterlyScores.show
//    // ordering and empty rows done manually for demo purposes
//    quarterlyScores.rollup("period", "student").sum("score").show
//
//
//    // using struct function
//    inventory.rollup(struct("item", "color") as "(item,color)").sum().show
//
//    // using expr function
//    inventory.rollup(expr("(item, color)") as "(item, color)").sum().show
//
  }
  def rullupTest(spark:SparkSession)={
    val expenses = spark.createDataFrame(Seq(
      ((2012, Month.DECEMBER, 12), 5),
      ((2016, Month.AUGUST, 13), 10),
      ((2017, Month.MAY, 27), 15))
      .map { case ((yy, mm, dd), a) => (LocalDate.of(yy, mm, dd), a) }
      .map { case (d, a) => (d.toString, a) }
      .map { case (d, a) => (Date.valueOf(d), a) }).toDF("date", "amount")

    // rollup time!
    val res = expenses
      .rollup(year(col("date")) as "year", month(col("date")) as "month")
      .agg(sum("amount") as "amount")
      .sort(col("year").asc_nulls_last, col("month").asc_nulls_last)
      .show()
  }
  def rullupAsGroupByTest(spark:SparkSession)={
    val sales = spark.createDataFrame(Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    )).toDF("city", "year", "amount")

    val groupByCityAndYear = sales
      .groupBy("city", "year")  // <-- subtotals (city, year)
      .agg(sum("amount") as "amount")

    val groupByCityOnly = sales
      .groupBy("city")          // <-- subtotals (city)
      .agg(sum("amount") as "amount")
      .select(col("city"), lit(null) as "year", col("amount"))  // <-- year is null
    //
    val withUnion = groupByCityAndYear
      .union(groupByCityOnly)
      .sort(desc_nulls_last("city"),asc_nulls_last("year"))

    withUnion.show()

    // 使用roll up函数
    val withRollup = sales
      .rollup("city", "year")
      .agg(sum("amount") as "amount")
      .sort(desc_nulls_last("city"), asc_nulls_last("year"))
      .filter(col("city").isNotNull.and(col("year").isNotNull))
      .select("city", "year", "amount")
    withRollup.show()


    // GROUPING SETS支持
    sales.createOrReplaceTempView("sales")

    val withGroupingSets = spark.sql("""
      SELECT city, year, SUM(amount) as amount
      FROM sales
      GROUP BY city, year
      GROUPING SETS ((city, year), (city))
      ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
      """).show()
  }
}

```