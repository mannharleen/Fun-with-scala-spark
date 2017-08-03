/*
  Created by Harleen Mann
    Create RDD
    Convert into WritableRDD
    Write a textFile to S3 using new hadoop API
    Read the file from S3 using new hadoop API
*/

import org.apache.hadoop.io._
import org.apache.lib.mapred._

val l = List((1,"a"),(2,"b"),(3,"c"),(4,"d"))
val rdd = sc.parallelize(l)
val rddWritable = rdd.map(x=> (new IntWritable(x._1), new Text(x._2)))

//Write using newApiHadoopFile to S3
rddWritable.saveAsNewAPIHadoopFile("s3n://<accessKey>:<secret>@<bucket>/test",
  classOf[IntWritable],
  classOf[Text],
  classOf[TextOutputFormat[IntWritable,Text]]
  )

//Read using newApiHadoopFile from S3 
sc.newAPIHadoopFile("s3n://<accessKey>:<secret>@<bucket>/test",	
  classOf[KeyValueTextInputFormat],
  classOf[Text],
  classOf[Text]).
    map(x=>(x._1.toString.toInt, x._2.toString)
  ).
    take(3)
