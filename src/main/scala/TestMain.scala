import org.apache.spark.{SparkContext,SparkConf}

/**
 * Created by ruveni on 6/28/15.
 */
object TestMain{

//  def main(args:Array[String]){
//    val conf=new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("Sigmoid Spark")
//      .set("spark.executor.memory","1g")
//      .set("spark.rdd.compress","true")
//      .set("spark.storage.memoryFraction","1")
//
//    val sc=new SparkContext(conf)
//
//    val data=sc.parallelize(1 to 10000000).collect().filter(_<1000)
//    data.foreach(println)
//  }

  def main(args: Array[String]) {
    val inputFile = "README.txt"
    val outputFile = "LOVE"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}