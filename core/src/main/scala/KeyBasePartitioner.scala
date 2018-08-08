
import org.apache.spark.Partitioner

class KeyBasePartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    Math.abs(k.hashCode() % numPartitions)
  }
}
