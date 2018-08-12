
# OrderedRDDFunctions

针对key value类型的RDD的一些函数，这里会针对key进行隐式排序。

对与任意类型 K 的key，只要有Ordering[K]的隐式转换，这些函数就能正常工作。

对于原始类型，无需指定Ordering[K]的隐式转换，因为已经默认存在。

用户可以为自己的自定义类型定义自己的orderings，或者也可以覆盖默认的排序。

会就近使用隐式的orderings。

RDD隐式转换调用的过程是在object RDD

```scala
implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
    : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }
```

## sortByKey

使用key对rdd进行排序，目的是使得每个分区的原素是有序的。

对结果rdd调用，collect或者save，会返回或者输出一个排序的list。针对save会输出多个part-X文件，并且文件内key是有序的。

```scala

    import org.apache.spark.SparkContext._
    sc.textFile("file:///opt/hadoop/spark-2.3.1/README.md").flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).map(each=>(each._2,each._1))
    implicit val caseInsensitiveOrdering = new Ordering[Int] {
     override def compare(a: Int, b: Int) = b.compareTo(a)
    }
    // Sort by key, using 
     res0.sortByKey().saveAsTextFile("file:///opt/output/")

    res1结果会和没有覆盖orderings，并将参数设置为false的结果一样。
    res0.sortByKey(false).saveAsTextFile("file:///opt/output/")
```

## filterByRange

该函数返回的RDD的原素仅仅是在key的lower和upper范围内的。

假如该RDD已经使用了RangePartitioner进行重分区，那么该操作将会非常高效，因为它只需要扫描那些包含这些原素的RDD。

```scala

    import org.apache.spark.SparkContext._
    sc.textFile("file:///opt/hadoop/spark-2.3.1/README.md").flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).map(each=>(each._2,each._1))
    implicit val caseInsensitiveOrdering = new Ordering[Int] {
     override def compare(a: Int, b: Int) = a.compareTo(b)
    }
    // Sort by key, using 
     res0.filterByRange(35,50).take(10)

    返回值只有 47,注意，这里的caseInsensitiveOrdering与前面的已经不同
```

## repartitionAndSortWithinPartitions

使用给定的分区器对RDD进行重分区，并且会使用key对每个分区进行排序。

这个比先调用repartition，然后再调用sortByKey更加高效。

因为其将排序下推到shuffle的过程。

```scala
  import org.apache.spark.Partitioner
  class KeyBasePartitioner(partitions: Int) extends Partitioner {

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[Int]
      Math.abs(k.hashCode() % numPartitions)
    }
  }
  
  import org.apache.spark.SparkContext._
      sc.textFile("file:///opt/hadoop/spark-2.3.1/README.md").flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).map(each=>(each._2,each._1))
      implicit val caseInsensitiveOrdering = new Ordering[Int] {
       override def compare(a: Int, b: Int) = b.compareTo(a)
      }
      // Sort by key, using 
  res7.repartitionAndSortWithinPartitions(new KeyBasePartitioner(3)).saveAsTextFile("file:///opt/output/")
  
```
结果,可以看到每个分区都是有效的。
```scala
meitudeMacBook-Pro-3:output meitu$ pwd
/opt/output
meitudeMacBook-Pro-3:output meitu$ ls
_SUCCESS        part-00000      part-00001      part-00002
meitudeMacBook-Pro-3:output meitu$ head -n 10 part-00000 
(24,the)
(12,for)
(9,##)
(9,and)
(6,is)
(6,in)
(3,general)
(3,documentation)
(3,example)
(3,how)
meitudeMacBook-Pro-3:output meitu$ head -n 10 part-00001
(16,Spark)
(7,can)
(7,run)
(7,on)
(4,build)
(4,Please)
(4,with)
(4,also)
(4,if)
(4,including)
meitudeMacBook-Pro-3:output meitu$ head -n 10 part-00002
(47,)
(17,to)
(8,a)
(5,using)
(5,of)
(2,Python)
(2,locally)
(2,This)
(2,Hive)
(2,SparkPi)
meitudeMacBook-Pro-3:output meitu$ 
```

