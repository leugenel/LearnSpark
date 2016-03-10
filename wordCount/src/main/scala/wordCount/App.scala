package wordCount


import java.io.{IOException, File}

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD


object WordCount {

  //To work with input and output file need add to configuration program arguments Faust.txt output.txt
  def main(args: Array[String]) {

    object logger extends Serializable {
      @transient lazy val log = Logger.getLogger(getClass.getName)
    }

    //val logger  =  Logger.getLogger(getClass.getName)
    var inputFile  :String = null
    var outputFile :String = null

    args.length match {
      case 0 => logger.log.info("No arguments are defined. Continue with the code defined.")

      //assume that output file is defined
      case 1 => outputFile = args(0)

      case 2 => inputFile = args(0)
                outputFile = args(1)
      //default
      case _ => logger.log.error("More arguments than expected. Exit.")
                return
    }

    //The HADOOP_HOME system variable should be set
    //In this path /bin placed winutils.exe required

    if(outputFile!=null){
       val file = new File(System.getProperty("user.dir")+"//"+outputFile)
       logger.log.info("Directory: "+System.getProperty("user.dir")+"//"+outputFile)
       if(file.isDirectory) {
         try {
           FileUtils.deleteDirectory(file)
           logger.log.info("Directory deleted")
         }
         catch{
           case ioe:IOException => logger.log.error("Directory deletion is failed")
         }
       }
       else{
         logger.log.debug("No directory find")
       }
    }

    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
    var input :RDD[String] = null
    if(inputFile!=null) {
       input = sc.textFile(inputFile)
    }
    else {
       input = sc.parallelize(List("pandas!", "i like pandas!!!"))
    }
    // Split up into words. Use " ", "!" and other signs to split
    val words = input.flatMap(line => line.split(" |\\!|\\.|\\:|\\;|\\,|\\)|\\("))


    // Transform into word and count.

    //Original
    //val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}

    //In this way we sort by value - doing swap and then sort
    //Set filter to remove length>2
    val counts = words.map(word =>
                          (word, 1)).reduceByKey{case (x, y) => x + y}.map(item => item.swap).sortByKey().filter{case(x,y) => y.length>2}

    //Lets print only words that length > 2 and not contains '.'
    //val countsMore2 = counts.filter{case(x,y) => y.length>2 && !y.contains('.')}

    //counts.foreach((t) => logger.log.info("(" + t._1 + "," + t._2 + ")"))
    counts.foreach((t) => logger.log.info("(" + t._1 + "," + t._2 + ")"))

    // Save the word count back out to a text file, causing evaluation.
    if(outputFile!=null) {
      //counts.saveAsTextFile(outputFile)
      counts.saveAsTextFile(outputFile)
    }
  }

}


