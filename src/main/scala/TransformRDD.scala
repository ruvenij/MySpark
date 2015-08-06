import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ruveni on 7/17/15.
 */
object TransformRDD {
  def main(args:Array[String]) {
    //map - one element one output
    val conf=new SparkConf().setAppName("Laridae").setMaster("local")
    val sc=new SparkContext(conf)
    val input=sc.parallelize(List(2,4,6,8))
    val resultt=input.map(x=>x*x)
    //persistence
    resultt.persist(StorageLevel.DISK_ONLY)
    println(input.collect().mkString(","))
    println(resultt.count())

    //flatmap - one element multiple outputs
    val lines=sc.parallelize(List("My Love","Mother"))
    val words=lines.flatMap(line=>line.split(" "))
    println("First Word  "+words.first())

    val flat=input.flatMap(x=>x.to(5))
    println(flat.collect().mkString(","))

    //aggregate
    val result = input.aggregate((0, 0)) ((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    val avg = result._1 / result._2.toDouble
    println(result)
  }
}
