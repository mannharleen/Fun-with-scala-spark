import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
val conf = new SparkConf().setAppName("Testing").setMaster("local")
val sc = new SparkContext(conf)

sc.textFile("C:\\Users\\Administrator\\data\\retail_db\\orders\\part-00000").map(x=> (x.split(",")(3),1)).reduceByKey((x,y) => x+y).collect().foreach(println)
