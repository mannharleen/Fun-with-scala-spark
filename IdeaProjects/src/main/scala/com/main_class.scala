package com
import com.typesafe.config.ConfigFactory

object main_class {
  def main(args: Array[String]): Unit = {
    //initialize spark
    //spark_init.spark_init.init()

    //get s3 credentials
    val conf = ConfigFactory.load
    val aKey = conf.getString("s3.accessKey")
    val sKey = conf.getString("s3.secretKey")

    //get oracle credentails

    //perform ETL


  }

}
