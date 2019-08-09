package org.apache.spark.examples.sql.streaming

import org.apache.spark.sql.types.StructField

object java2scala {

  def java2scala(fields: Array[StructField]):Array[StructField]= {
    import scala.collection.JavaConversions._
    // $example on:programmatic_schema$
    fields
  }
}
