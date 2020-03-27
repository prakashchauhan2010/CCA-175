package retail_db

import java.text.SimpleDateFormat



import common.ConnectionUtil
import org.apache.spark.sql.SaveMode

/**
 * Join the data at hdfs location /user/edureka_353264/prakash/practice1/exam1/question4/orders &
 * /user/edureka_353264/prakash/practice1/exam1/question4/customers
 * to find out customers who have placed more than 4 orders.
 *
 * Schema for customer File
 * customer_id,customer_fname,...
 *
 * Schema for Order File
 * order_id,order_date,order_customer_id,order_status
 *
 * Output Requirement:
 * •	Order status should be COMPLETE
 * •	Output should have customer_id,customer_fname,count
 * •	Save the results in json format.
 * •	Result should be saved in /user/edureka_353264/prakash/practice1/exam1/question4/orders /output
 *
 */
object ProblemStatement4 extends App {
  val sc = ConnectionUtil.sc
  val spark = ConnectionUtil.spark
  import spark.implicits._

  case class Customer(customer_id: Int, customer_fname: String)
  case class Order(order_id: Int, order_date: Long, order_customer_id: Int, order_status: String)

  var customer = sc.textFile("data-files//retail_db//customers")
    .map(line => {
      var details = line.split(",")
      Customer(details(0).trim().toInt, details(1).toString().trim())
    }).toDS()

  customer.createOrReplaceTempView("cust")

  var orders = sc.textFile("data-files//retail_db//orders")
    .map(line => {
      var details = line.split(",")
      Order(details(0).trim().toInt, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(details(1).trim()).getTime(), details(2).trim().toInt, details(3).trim())
    }).toDF()

  orders.createOrReplaceTempView("orders")

  //  val filtered_orders = spark.sql("select * from orders where order_status = 'COMPLETE'")
  //  filtered_orders.createOrReplaceTempView("filtered_orders")

  val result = spark.sql("""
            SELECT c.customer_id, 
               c.customer_fname,
               Count(1) count 
            FROM   cust c 
                   JOIN orders o 
                     ON c.customer_id = o.order_customer_id 
            WHERE  o.order_status = 'COMPLETE'
            GROUP  BY customer_id, 
                      customer_fname
            ORDER  BY Count(1) DESC 
    """)
  
    result.write.format("json").mode(SaveMode.Overwrite).save("data-files//retail_db//ps4")
    println("Record Count: "+result.count())
}