package com.exercise07

/*
Problem Statement: (wrt retail_db)
Get the total revenue for each day ordered by date
Print order_date, order_revenue
Use sparkSQL in native mode
 */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, functions}

case class orders_case_class(
                            order_id:Int,
                            order_date:String,
                            order_customer_id:Int,
                            order_status:String
                            )
                            
case class order_items_case_class(
                                   order_item_id:Int,
                                   order_item_order_id:Int,
                                   order_item_product_id:Int,
                                   order_item_quantity:Int,
                                   order_item_subtotal:Double,
                                   order_item_product_price:Double
                                 )
                                 
object sqlJoins {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sparksqlJoins").setMaster("local[1]"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions","2")  //set this to an optimum num, else default is 200!

    val ip_orders = sc.textFile("s3n://XXX:YYY@modharleen/bigdata/data/retail_db/orders/")
    val ip_order_items = sc.textFile("s3n://XXX:YYY@modharleen/bigdata/data/retail_db/order_items/")

    //create df from rdd
    val df_orders = ip_orders.map(x=> {
        val t = x.split(",")
        orders_case_class(t(0).toInt,t(1),t(2).toInt,t(3))
    }).toDF()
    val df_order_items = ip_order_items.map(x=> {
      val t = x.split(",")
      order_items_case_class(t(0).toInt,t(1).toInt,t(2).toInt,t(3).toInt,t(4).toDouble, t(5).toDouble)
    }).toDF()

    //register temp tables
    df_orders.registerTempTable("orders")

    df_order_items.registerTempTable("order_items")

    val df_out = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal) order_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id group by o.order_date")

    df_out.orderBy("order_date").collect()

  }

}

/*
Output:
+--------------------+------------------+
|          order_date|               _c1|
+--------------------+------------------+
|2013-07-25 00:00:...| 68153.83000000005|
|2013-07-26 00:00:...|136520.17000000013|
|2013-07-27 00:00:...|101074.34000000008|
|2013-07-28 00:00:...| 87123.08000000006|
|2013-07-29 00:00:...|137287.08999999997|
|2013-07-30 00:00:...|102745.62000000008|
|2013-07-31 00:00:...| 131878.0600000002|
|2013-08-01 00:00:...|129001.62000000008|
....

*/
