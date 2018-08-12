
## 1. mappartition的妙用

本问主要想讲如何高效的使用mappartition。

首先，说到mappartition大家肯定想到的是map和MapPartition的对比。网上这类教程很多了，以前浪尖也发过类似的，比如

[对比foreach和foreachpartition](https://mp.weixin.qq.com/s/Pz48-M4FSxGjIt9G9NNzTQ)

主要是map和foreach这类的是针对一个元素调用一次我们的函数，也即是我们的函数参数是单个元素，假如函数内部存在数据库链接、文件等的创建及关闭，那么会导致处理每个元素时创建一次链接或者句柄，导致性能底下，很多初学者犯过这种毛病。

而foreachpartition是针对每个分区调用一次我们的函数，也即是我们函数传入的参数是整个分区数据的迭代器，这样避免了创建过多的临时链接等，提升了性能。

下面的例子都是1-20这20个数字,经过map或者MapPartition然后返回a*3。

## 2. map的使用

```scala
val a = sc.parallelize(1 to 20, 2)
def mapTerFunc(a : Int) : Int = {
    a*3
}
val mapResult = a.map(mapTerFunc)

println(mapResult.collect().mkString(","))
```
结果

```scala
3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60
```


## 3. mappartition低效用法

```scala
  val a = sc.parallelize(1 to 20, 2)
  def terFunc(iter: Iterator[Int]) : Iterator[Int] = {
    var res = List[Int]()
    while (iter.hasNext)
    {
      val cur = iter.next;
      res.::= (cur*3) ;
    }
    res.iterator
  }
val result = a.mapPartitions(terFunc)
println(result.collect().mkString(","))
```

结果

```scala
30,27,24,21,18,15,12,9,6,3,60,57,54,51,48,45,42,39,36,33
```

## 4. mappartition的高效用法

注意，3中的例子，会在mappartition执行期间，在内存中定义一个数组并且将缓存所有的数据。假如数据集比较大，内存不足，会导致内存溢出，任务失败。
对于这样的案例，Spark的RDD不支持像mapreduce那些有上下文的写方法。下面有个方法是无需缓存数据的，那就是自定义一个迭代器类。

```scala
class CustomIterator(iter: Iterator[Int]) extends Iterator[Int] {
    def hasNext : Boolean = {
      iter.hasNext
    }

    def next : Int= {
    
    val cur = iter.next
    
     cur*3
    }
  }
  
  val result = a.mapPartitions(v => new CustomIterator(v))
  println(result.collect().mkString(","))
```
结果：
```scala
3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60
```