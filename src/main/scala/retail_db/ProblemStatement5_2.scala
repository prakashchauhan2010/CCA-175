package retail_db

import common.ConnectionUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.io.compress.GzipCodec

object ProblemStatement5_2 extends App {
  val sc = ConnectionUtil.sc
  val session = ConnectionUtil.spark
  import session.implicits._

  val schema = StructType(
    Array(
      StructField("product_id", IntegerType, false),
      StructField("product_category_id", IntegerType, false),
      StructField("product_name", StringType, false),
      StructField("product_description", StringType, false),
      StructField("product_price", DoubleType, false),
      StructField("product_image", StringType, false)))

  val prodDF = session.read.schema(schema).csv("data-files//retail_db//products")
  prodDF.createOrReplaceTempView("products")
  val result = session.sql("""
        SELECT product_category_id, 
           Max(product_price) maximum_price
        FROM   products 
        GROUP  BY product_category_id 
        ORDER  BY Max(product_price) DESC
    """)

  result.rdd.repartition(1).map(row => row.mkString("|")).saveAsTextFile("data-files//retail_db//ps5_2", classOf[GzipCodec])

  prodDF.printSchema()
}