package com.cs441.anand.MapReduce

import com.cs441.anand.Utils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.util.matching.Regex

/* computes distribution of different types of messages across predefined time intervals
 and injected string instances of the designated regex pattern for these log message types */
class MapReduce1

/** Factory for [[MapReduce1]] instances */
object MapReduce1 {
  // get the config reference object
  val config = ObtainConfigReference("MapReduce1") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // create a logger for this class
  val logger = CreateLogger(classOf[MapReduce1.type])

  /** Custom Mapper class */
  class CountMapper extends Mapper[Object, Text, Text, IntWritable] {
    // errorType - key, count(1) - value
    val errorType = new Text()

    /** Override map function
     * @param key : Object
     * @param value : Text
     * @param context : Mapper[Object, Text, Text, IntWritable]
     * @return Unit - write (errorType, 1)
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // errorType - key, count(1) - value
      val line = value.toString().split(' ')

      // create a DateTimeFormatter object
      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

      // get time interval and string pattern from application.conf file
      val startTime = LocalTime.parse(config.getString("MapReduce1.startTime"), formatter)
      val endTime = LocalTime.parse(config.getString("MapReduce1.endTime"), formatter)
      val stringPattern: Regex = config.getString("MapReduce1.stringPattern").r

      // get message and time from input, parse time to DateTimeFormatter object
      val message = line.last
      val time = LocalTime.parse(line(0), formatter)

      // check if the pattern is present
      val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
        case Some(_) => true
        case None => false
      }

      logger.info("Pattern present: " + isPatternPresent + " Message: " + message + "\n")
      logger.info("Before: " + startTime.isBefore(time) + "\n")
      logger.info("After: " + endTime.isAfter(time) + "\n")

      // if the pattern is present and is within specified time interval
      if (startTime.isBefore(time) && endTime.isAfter(time) && isPatternPresent) {
        // set error type from the input
        errorType.set(line(2))

        // write error type as key and count(1) as value
        context.write(errorType, new IntWritable(1))
      }
    }
  }

  /** Custom Partitioner class */
  class CustomPartitioner extends Partitioner[Text,IntWritable] {
    /** Override reduce function - aggregate count for each interval
     * @param key : Text - error type
     * @param value: IntWritable - value 1
     * @param numReduceTasks : Int
     * @return Unit - write (interval, count)
     */
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      // split the input from the mapper
      val errorType = value.toString().split("\t")(0)

      // if the number of reduce tasks is 0,
      if (numReduceTasks == 0) return 0

      // if the error type is INFO, assign to second reducer
      if (errorType == "INFO") return 1 % numReduceTasks

      // assign other error types to first reducer
      return 0
    }
  }

  /** Custom Reducer class */
  class CountReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    /** Override reduce function - aggregate count for each interval
     * @param key : Text - interval
     * @param values: Iterable[IntWritable] - collection of 1s for each interval
     * @param context : Reducer[Text, IntWritable, Text, IntWritable]
     * @return Unit - write (interval, count)
     */
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // aggregate the 1s for each key
      val sum = values.asScala.foldLeft(0)(_ + _.get)

      // write key, and sum as value
      context.write(key, new IntWritable(sum))
    }
  }

  /** Method to start MapReduce1 - called by Driver class' main method
   * @param args : Array[String] - command line input
   * @return Unit - Writes (interval, count) sorted descending
   */
  def start(args: Array[String]): Unit = {
    logger.info("MapReduce1 starting...\n")

    // Read the job's configuration of the cluster from configuration xml files
    val configuration = new Configuration

    // Initialize job with default configuration of the cluster
    val job = Job.getInstance(configuration,"Log Gen Map Reduce")

    // Assign the driver class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper, Combiner and Reducer class
    job.setMapperClass(classOf[CountMapper])
    job.setCombinerClass(classOf[CountReducer])
    job.setReducerClass(classOf[CountReducer])

    // Assign custom partitioner class
    job.setPartitionerClass(classOf[CustomPartitioner])

    // Set number of reduce tasks to 2
    job.setNumReduceTasks(2)

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
