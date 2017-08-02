package com.dataframeJoin

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object dataframeJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dataframejoin").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions","2")
    import sqlContext.implicits._

    val table1 = List("1,table1.one","2,table1.two")
    val table2 = List("1,table2.one","2,table2.two")

    val df1 = sc.parallelize(table1).map(x=> {
      val temp = x.split(",")
      caseClassTable1(temp(0).toInt,temp(1))
    }
    ).toDF

    val df2 = sc.parallelize(table2).map(x=> {
      val temp = x.split(",")
      caseClassTable1(temp(0).toInt,temp(1))
    }
    ).toDF

    df1.show
    df2.show

    //inner join
    val dfjoin1 = df1.join(df2,df1("a") === df2("a"))
    dfjoin1.show

    //since column names are same, join should be achieved using:
    //inner join
    val dfjoin2 = df1.join(df2,"a")
    dfjoin2.show

    //outer join
    val dfOuterJoin = df1.join(df2,Seq("a"),"leftouter")
    dfOuterJoin.show

    //Select columns
    dfjoin1.select(dfjoin2("a")).show()

    //Filter
    dfjoin1.filter(dfjoin2("a") === 2)

    //Using spark SQL
    dfjoin2.registerTempTable("joinedTable")
    sqlContext.sql("select * from joinedTable").show()
  }
}


