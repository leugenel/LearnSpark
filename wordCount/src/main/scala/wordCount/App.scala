package wordCount

import org.apache.spark.SparkContext._
import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.

    //Original
    //val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}

    //Sort by value
    //val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}.sortByKey()

    //In this way we sort by value
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}.map(item => item.swap).sortByKey()


    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}


