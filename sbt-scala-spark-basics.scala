import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val conf = new SparkConf().setAppName("GKL").setMaster("local[2]")
val sc = new SparkContext(conf)
sc.setLogLevel("INFO")

sc.textFile("C:\\Users\\XX\\data\\read_data.txt").flatMap(x=>x.split(" ")).map(x=> (x,1)).reduceByKey((x,y)=> x+y).collect().foreach(println)
