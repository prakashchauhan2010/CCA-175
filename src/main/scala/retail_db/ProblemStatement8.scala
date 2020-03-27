package retail_db

import common.ConnectionUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode

/**
 * 
 * When reading data using HiveContext, below configuration does not work:
 * hc.setConf("compression", "gzip") 
 * also session.conf.set("compression", "gzip")
 * 
 * Only setting compression in dataframe.write works
 * data.write.option("compression", "gzip")
 * 
 */
object ProblemStatement8 extends App {
  val sc = ConnectionUtil.sc
  val session = ConnectionUtil.spark
  import session.implicits._

  // Need to load data in Hive table first.
  val hc = new HiveContext(sc)
  val data = session.sql("select * from retail_db_prakash.product_replica where product_price > 100")
  //  hc.setConf("compression", "gzip") // This does not work when HiveContext is used.
  data.write.mode(SaveMode.Overwrite).option("compression", "gzip").parquet("data-files//retail_db//ps8")

}