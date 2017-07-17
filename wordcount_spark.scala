sc.textFile("C:\\Users\\XX\\data\\read_data.txt").flatMap(x=>x.split(" ")).map(x=> (x,1)).reduceByKey((x,y)=> x+y).collect().foreach(println)
