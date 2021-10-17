package com.cs441.anand.MapReduce

import com.cs441.anand.Utils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import scala.collection.JavaConverters._
import scala.util.matching.Regex

/* computes the number of characters in each log message for each type that contain the
 highest number of characters in the detected instances of the designated regex pattern */
class MapReduce4

/** Factory for [[MapReduce4]] instances */
object MapReduce4 {
  // get the config reference object
  val config = ObtainConfigReference("MapReduce4") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // create a logger for this class
  val logger = CreateLogger(classOf[MapReduce4.type])

  /** Custom Mapper class */
  class MaxMapper extends Mapper[Object, Text, Text, IntWritable] {
    // errorType - key, length of message - value
    val errorType = new Text()
    val length = new IntWritable()

    /** Override map function
     * @param key : Object
     * @param value : Text
     * @param context : Mapper[Object, Text, Text, IntWritable]
     * @return Unit - write (errorType, 1)
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(' ')

      // get string pattern from application.conf file
      val stringPattern: Regex = config.getString("MapReduce4.stringPattern").r

      // get message from input
      val message = line.last

      // check if the pattern is present
      val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
        case Some(_) => true
        case None => false
      }

      logger.info("Pattern present: " + isPatternPresent + " Message: " + message + "\n")

      // if the pattern is present
      if (isPatternPresent) {
        // set error type from the input
        errorType.set(line(2))

        // compute length - the number of characters in the message
        length.set(message.length)

        // write error type as key and length as value
        context.write(errorType, length)
      }
    }
  }

  /** Custom Reducer class */
  class MaxReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    // result - value, compute max and store it in result
    val result = new IntWritable()

    /** Override reduce function - aggregate count for each interval
     * @param key : Text - interval
     * @param values: Iterable[IntWritable] - collection of 1s for each interval
     * @param context : Reducer[Text, IntWritable, Text, IntWritable]
     * @return Unit - write (interval, count)
     */
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // find the maximum value from the collection values
      val max = values.asScala.foldLeft(0)(_ max _.get)

      // set the max to result
      result.set(max)

      // write key, and result as value
      context.write(key, result)
    }
  }

  /** Method to start MapReduce4 - called by Driver class' main method
   * @param args : Array[String] - command line input
   * @return Unit - Writes (interval, count) sorted descending
   */
  def start(args: Array[String]): Unit = {
    logger.info("MapReduce4 starting...\n")

    // Read the job's configuration of the cluster from configuration xml files
    val configuration = new Configuration

    // Initialize job with default configuration of the cluster
    val job = Job.getInstance(configuration,"Log Gen Map Reduce")

    // Assign the driver class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper, Combiner and Reducer class
    job.setMapperClass(classOf[MaxMapper])
    job.setCombinerClass(classOf[MaxReducer])
    job.setReducerClass(classOf[MaxReducer])

    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))

    // Wait till job1 completes
    job.waitForCompletion(true)

    logger.info("Job completed...\n")

    // Exit after completion
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }
}
