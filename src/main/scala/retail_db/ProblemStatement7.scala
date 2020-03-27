package retail_db

import common.ConnectionUtil
import com.databricks.spark.avro._
import org.apache.hadoop.io.compress.GzipCodec

object ProblemStatement7 extends App {
  val sc = ConnectionUtil.sc
  val session = ConnectionUtil.spark
  import session.implicits._

  val cust = session.read.avro("data-files//retail_db//customers_avro")
  cust.rdd.map(array => array.mkString("\t"))
    .saveAsTextFile("data-files//retail_db//ps7", classOf[GzipCodec])
}