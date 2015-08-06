import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ruveni on 8/6/15.
 */
object MyTest {
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("Test").setMaster("local")
    val sc=new SparkContext(conf)
    val input=sc.parallelize(List(1,2,3,4,5))
    println(input.sum());


  }
}
