package retail_db

import common.ConnectionUtil
import org.apache.spark.sql.SaveMode

object ProblemStatement5_1 extends App {
  val sc = ConnectionUtil.sc
  val session = ConnectionUtil.spark
  import session.implicits._

  case class Product(product_id: Int, product_category_id: Int, product_price: Double)
  val productFile = sc.textFile("data-files//retail_db//products")
  val prodDS = productFile.map(line => {
    val data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    Product(data(0).trim().toInt, data(1).trim().toInt, data(4).trim().toDouble)
  }).toDS()
  prodDS.na.fill(0)
  prodDS.show()
  prodDS.printSchema()

  prodDS.createOrReplaceTempView("products")
  val result = session.sql("""
        SELECT product_category_id, 
           Max(product_price) maximum_price
        FROM   products 
        GROUP  BY product_category_id 
        ORDER  BY Max(product_price) DESC
    """)

  result.show()
  result.rdd.repartition(1).map(row => row.mkString("|")).saveAsTextFile("data-files//retail_db//ps5")
}