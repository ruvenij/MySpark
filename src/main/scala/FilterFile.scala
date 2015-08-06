import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ruveni on 7/17/15.
 */
object FilterFile {
  def main(args:Array[String]){
    val conf=new SparkConf().setMaster("local").setAppName("Laridae")
    val sc=new SparkContext(conf)
    val inputFile="README.txt"
    val input=sc.textFile(inputFile)
    val loveLine=input.filter(line=>line.contains("love"))
    val youLine=input.filter(line=>line.contains("you"))
    val unionLines=loveLine.union(youLine)
    //unionLines.saveAsTextFile("MyLines")
    println("GGG            "+unionLines.count())
    println("First 10 examples")
    unionLines.take(10).foreach(line=>println(line))

  }
}
