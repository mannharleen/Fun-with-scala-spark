import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object sparkSql_basics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions","2")
    //val hiveContext = new HiveContext(sc) //not used in this code

    //create rdd
    val rdd = sc.parallelize(List((1,"one"),(2,"two")))
    //convert to df
    import sqlContext.implicits._
    val df = rdd.toDF("col1","col2")
    //sql operations
    df.registerTempTable("table1")
    sqlContext.sql("select * from table1").show()

  }

}
