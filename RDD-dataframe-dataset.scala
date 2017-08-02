/*
Created by Harleen Mann
    
*/

/************************************************************************************/
//create RDD
val ip = sc.parallelize(List("1 First line","2 Second line","3 Third line"))

val ipMap =
ip.map(x=> {
    val y = x.split(" ")
    mapClass(y(0).toInt,y(1),y(2))
    })
/************************************************************************************/
//create dataframe from RDD
case class mapClass(a:Int, b:String, c:String)
val df = ipMap.toDF

df.registerTempTable("table1")

val sql1 = sqlContext.sql("select * from table1")
val sql2 = sqlContext.sql("select * from table1 where a>=2")
    //sql.DataFrame = [a: int, b: string, c: string]

sql1.show()
sql2.show()
/************************************************************************************/
//Create dataframe directly
sqlContext.read.json("file:///home/cloudera/file1.json")
    //sql.DataFrame = [a: bigint, b: string, c: string] 

/************************************************************************************/
//create dataset from dataframe
case class mapClass1(a:Long, b:String, c:String)
val ds = sqlContext.read.json("file:///home/cloudera/file1.json").as[mapClass1]
    //spark.sql.Dataset[mapClass1] = [a: bigint, b: string, c: string]
ds.show()

