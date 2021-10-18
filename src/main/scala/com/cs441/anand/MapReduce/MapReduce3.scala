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

/* computes the number of the generated log messages for each message type */
class MapReduce3

/** Factory for [[MapReduce3]] instances */
object MapReduce3 {
  // create a logger for this class
  val logger = CreateLogger(classOf[MapReduce3.type])

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
      // split the input line by the delimiter ' '
      val line = value.toString().split(' ')

      // set error type from the input
      errorType.set(line(2))

      // write error type as key and count(1) as value
      context.write(errorType, new IntWritable(1))
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

  /** Method to start MapReduce3 - called by Driver class' main method
   * @param args : Array[String] - command line input
   * @return Unit - Writes (interval, count) sorted descending
   */
  def start(args: Array[String]): Unit = {
    logger.info("MapReduce3 starting...\n")

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
