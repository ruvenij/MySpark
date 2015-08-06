import org.apache.spark.rdd.RDD
import org.apache.spark.{rdd, SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by ruveni on 7/18/15.
 */
object PairRDD {
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("Laridae").setMaster("local")
    val sc=new SparkContext(conf)
    val input=sc.textFile("README.txt")
    val pairs=input.map(line=>(line.split(" ")(0),line))
    pairs.filter{case(key,value)=>value.length<20}
    pairs.foreach(line=>println(line))

    //aggregate
    val words=input.map(line=>(line.split(" ")(0),1)).reduceByKey((x,y)=>(x+y))
    //words.reduceByKey((x,y)=>(x+y))
    words.foreach{case(key,value)=>println(key+" "+value)}

    //average
    val numberFile=sc.textFile("Numbers.txt")
    val numbers=numberFile.flatMap(line=>line.split(" "))
    val r=scala.util.Random
    val count=numbers.map(number=>(number,r.nextInt(10)))

    val result = count.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().map(println(_))

    result.foreach(num=>println(num))

    //
    val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
    val c = b.zip(a)
    val d = c.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
    d.collect
    d.foreach(line=>println(line))

    //nothing happens
    val data = Seq(("a", 3), ("b", 4), ("a", 1),("b",5),("c",3),("a",9),("d",1),("d",3))
    //sc.parallelize(data).reduceByKey((x, y) => x + y)    // Default parallelism
    //sc.parallelize(data).reduceByKey((x, y) => (x + y))    // Custom parallelism
    //data.foreach(line=>println(line))

    //sorting
    data.toList.sorted foreach{case(key,value)=>println(key+" "+value)}
    println()
    data.toList.sortBy(_._2) foreach{case(key,value)=>println(key+" "+value)}
    println(sc.parallelize(data).countByKey())
    println(sc.parallelize(data).lookup("d"))

  }
}
