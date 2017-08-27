/*
Problem Statement

Develop word count program using Scala IDE
Read the data from files and write the word count output to files
Make sure input and output paths are passed as arguments
Use the scala interpreter and preview the data after each step using Spark APIs
Test it on your PC
Build jar file
Submit jar file to a spark cluster
*/

package com.exercise03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //val inprdd = sc.textFile("file://C:/Users/harleen.singh/Downloads/ChargeCode.txt")
    val inprdd = sc.textFile(args(1))
    val outrdd = inprdd.flatMap(x=> x.split("\t")).map(x=> (x,1)).reduceByKey( (x,a)=> x+a)
    val outrddfile = outrdd.map(x => x._1 + " - " +  x._2.toString)
    //outrddfile.saveAsTextFile("file:///C:/Users/harleen.singh/Downloads/outputChargeCode")
    outrddfile.saveAsTextFile(args(2))

  }

}
