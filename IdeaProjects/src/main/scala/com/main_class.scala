package com
import com.typesafe.config.ConfigFactory

object main_class {
  def main(args: Array[String]): Unit = {
    //initialize spark
    val (sc,sqlContext) = spark_init.spark_init.init()
    import sqlContext.implicits._

    //get s3 credentials
    val conf = ConfigFactory.load
    val s3aKey = conf.getString("s3.accessKey")
    val s3sKey = conf.getString("s3.secretKey")

    //get oracle credentails
    val fisicienUatUname = conf.getString("fisicienuat.uname")
    val fisicienUatPName = conf.getString("fisicienuat.pword")

    //perform ETL

    //get source data
    import java.util.Properties
    val prop = new Properties()
    prop.setProperty("user",fisicienUatUname)
    prop.setProperty("password",fisicienUatPName)
    prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
      //manually partitioning right now
    val table_beat = "fscphkl.boepisodeaccounttxn"
    val df_beat = sqlContext.read.jdbc("jdbc:oracle:thin:@//172.18.20.70:1521/FSCLIVE",table_beat,
      "boepisodeaccounttxn_id",1,100000000,4, prop)

    //put to destination
    df_beat.write.parquet(s"s3n://$s3aKey:$s3sKey@modharleen/fisicien_uat/"+table_beat)





  }

}
