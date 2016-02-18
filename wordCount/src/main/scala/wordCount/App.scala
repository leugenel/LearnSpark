package wordCount


import java.io.{IOException, File}

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import org.apache.spark._


object WordCount {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val logger  =  Logger.getLogger(getClass.getName)

    //The HADOOP_HOME system variable should be set
    //In this path /bin placed winutils.exe required

    if(outputFile.length>0){
       val file = new File(System.getProperty("user.dir")+"//"+outputFile)
       logger.info("Directory: "+System.getProperty("user.dir")+"//"+outputFile)
       if(file.isDirectory) {
         try {
           FileUtils.deleteDirectory(file)
           logger.info("Directory deleted")
         }
         catch{
           case ioe:IOException => logger.error("Directory deletion is failed")
         }
       }
       else{
         logger.debug("No directory find")
       }
    }

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


