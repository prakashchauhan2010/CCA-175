package retail_db

import common.ConnectionUtil
import com.databricks.spark.avro._
import org.apache.spark.sql.SaveMode

/**
 * For Avro file format Spark 2.1.0 is compatible with databricks avro 3.2.0
 * defalte and snappy compression are available for Avro files.
 */
object ProblemStatement6 extends App {
  val sc = ConnectionUtil.sc
  val session = ConnectionUtil.spark

  val cust = session.read.option("delimiter", "\t")
    .option("inferSchema", true)
    .csv("data-files//retail_db//customers_tab_delimited")
    .toDF("customer_id", "customer_fname", "customer_city")

  cust.createOrReplaceTempView("customers")
  val result = session.sql("select * from customers where customer_city = 'Caguas'")
  session.conf.set("compression", "deflate")
  //  session.conf.set("spark.sql.avro.compression.codec", "deflate")
  result.write.mode(SaveMode.Overwrite).avro("data-files//retail_db//ps6")
}