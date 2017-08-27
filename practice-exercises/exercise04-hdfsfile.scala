/*
Problem Statement
Read data from HDFS and write the output back to HDFS
Run the jar file on the cluster in YARN mode using spark-submit

Notice the following:-
Number of stages used to run
Determine number of executors used to run
Determine number of tasks used to run for each stage
How many executor tasks ran by each executor in each stage?
Change the number of executors and observe the behavior
*/

package com.exercise04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object hdfsfile {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("hdfsfile").setMaster("local"))
    val ip = sc.textFile(args(0))
    ip.saveAsTextFile(args(1))

  }

}


/*

##spark-submit using default yarn deploy mode, which is client
spark-submit --class com.exercise04.hdfsfile \
--master yarn 
/home/cloudera/spark-jars/itv_exercises_2.10-1.0.jar "/user/cloudera/largefile" "/user/cloudera/largefileoutput"

##spark-submit using yarn with cluster deploy mode
spark-submit --class com.exercise04.hdfsfile \
--master yarn --deploy-mode cluster \
/home/cloudera/spark-jars/itv_exercises_2.10-1.0.jar "/user/cloudera/largefile" "/user/cloudera/largefileoutput"

*/
