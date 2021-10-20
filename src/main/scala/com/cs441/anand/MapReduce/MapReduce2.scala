package com.cs441.anand.MapReduce

import com.cs441.anand.Utils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.util.matching.Regex

/* computes time intervals sorted in the descending order, that contains most
 log messages of type ERROR with injected regex pattern string instances */
class MapReduce2

/** Factory for [[MapReduce2]] instances */
object MapReduce2 {
  // get the config reference object
  val config = ObtainConfigReference("MapReduce2") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // create a logger for this class
  val logger = CreateLogger(classOf[MapReduce2.type])

  /** Custom Mapper class - first mapper */
  class CountMapper extends Mapper[Object, Text, Text, IntWritable] {
    // interval - key, count(1) - value
    val interval = new Text()

    /** Override map function
     * @param key : Object
     * @param value : Text
     * @param context : Mapper[Object, Text, Text, IntWritable]
     * @return Unit - write (interval, 1)
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // split the input line by the delimiter ' '
      val line = value.toString().split(' ')

      // get string pattern from application.conf file
      val stringPattern: Regex = config.getString("MapReduce2.stringPattern").r

      // get the message and the type from the input
      val message = line.last
      val messageType = line(2)

      // check if the pattern is present
      val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
        case Some(_) => true
        case None => false
      }

      logger.info("Pattern present: " + isPatternPresent + " Message: " + message + "\n")

      // if the pattern is present and the message type is "ERROR"
      if (messageType == "ERROR" && isPatternPresent) {
        val time = line(0).toString().split('.')(0)
        interval.set(time)

        // write interval as key and count(1) as value
        context.write(interval, new IntWritable(1))
      }
    }
  }

  /** Custom Partitioner class */
  class CustomPartitioner extends Partitioner[Text, IntWritable] {
    /** Override reduce function - aggregate count for each interval
     * @param key : Text - error type
     * @param value: IntWritable - value 1
     * @param numReduceTasks : Int
     * @return Unit - write (interval, count)
     */
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      // split the input from the mapper
      val interval = value.toString().split("\t")(0)

      // get the seconds value
      val seconds = interval.split(":").last.toInt

      // if the number of reduce tasks is 0,
      if (numReduceTasks == 0) return 0

      // if the seconds value is between 45 and 60, assign to first reducer
      if (seconds >= 45 && seconds < 60) return 1 % numReduceTasks

      // if the seconds value is between 30 and 45, assign to second reducer
      else if (seconds >= 30 && seconds < 45) return 2 % numReduceTasks

      // if the seconds value is between 15 and 30, assign to third reducer
      else if (seconds >= 15 && seconds < 30) return 3 % numReduceTasks

      // by default, seconds value is between 00 and 15
      return 0
    }
  }

  /** Custom Reducer class - first reducer */
  class CountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
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

  /** Custom Mapper class - second mapper */
  class SwapMapper extends Mapper[Object, Text, Text, Text] {

    // create count, interval of type Text()
    val count = new Text()
    val interval = new Text()

    /** Override map function - Swap interval, count from first reducer to count, interval
     * @param key : Object
     * @param value : Text
     * @param context : Mapper[Object, Text, Text, Text]
     * @return : Unit - writes (count, interval)
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) : Unit = {
      // split the input line by the delimiter '\t'
      val line = value.toString().split('\t')

      // set values for count, interval
      count.set(line(1))
      interval.set(line(0))

      // write (count, interval)
      context.write(count, interval)
    }
  }

  /** Custom Comparator class */
  class CustomComparator extends WritableComparator (classOf[Text], true){
    /** Override compare function - descending order of counts before reducer
     * @param a: WritableComparable[_]
     * @param b: WritableComparable[_]
     * @return : Int - return -1 * (default output - ascending)
     */
    override def compare(a: WritableComparable[_], b: WritableComparable[_]) : Int = {
      return -1 * a.toString.compareTo(b.toString)
    }
  }

  /** Custom Reducer class - second reducer */
  class SortReducer extends Reducer[Text, Text, Text, Text] {
    /** Override reduce function
     * @param key : Text - count
     * @param values : Iterable[Text] - collection of intervals
     * @param context : Reducer[Text, Text, Text, Text]
     * @return Unit - Writes (interval, count) sorted descending
     */
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      // loop through each value in the collection of values
      values.asScala.foreach((value: Text) => {
        // write (value, key) not (key, value)
        context.write(value, key)
      })
    }
  }

  /** Method to start MapReduce2 - called by driver class' main method
   * @param args : Array[String] - command line input
   * @return Unit - Writes (interval, count) sorted descending
   */
  def start(args: Array[String]): Unit = {
    logger.info("MapReduce2 starting...\n")

    // Read job1's configuration of the cluster from configuration xml files
    val configuration1 = new Configuration

    // Initialize job1 with default configuration of the cluster
    val job1 = Job.getInstance(configuration1,"map/reduce task 2 - job1")

    // Assign the driver class to the job
    job1.setJarByClass(this.getClass)

    // Assign user-defined Mapper, Combiner and Reducer class
    job1.setMapperClass(classOf[CountMapper])
    job1.setCombinerClass(classOf[CountReducer])
    job1.setReducerClass(classOf[CountReducer])

    // Assign custom partitioner class
    job1.setPartitionerClass(classOf[CustomPartitioner])

    // Set number of reduce tasks to 4
    job1.setNumReduceTasks(4)

    // Set the Key and Value types of the output
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job1, new Path(args(1)))
    FileOutputFormat.setOutputPath(job1, new Path(args(2)))

    // Wait till job1 completes
    job1.waitForCompletion(true)

    logger.info("Job 1 completed...\n")

    // Read job2's configuration of the cluster from configuration xml files
    val configuration2 = new Configuration

    // Initialize job2 with default configuration of the cluster
    val job2 = Job.getInstance(configuration2,"map/reduce task 2 - job2")

    // Assign the driver class to the job
    job2.setJarByClass(this.getClass)

    // Assign user-defined Mapper, Combiner, and Reducer class
    job2.setMapperClass(classOf[SwapMapper])
    job2.setSortComparatorClass(classOf[CustomComparator])
    job2.setReducerClass(classOf[SortReducer])

    // Set the Key and Value types of the output
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])

    // Add input and output path from the args
    // input of mapper2 is the output of reducer1
    FileInputFormat.addInputPath(job2, new Path(args(2)))
    FileOutputFormat.setOutputPath(job2, new Path(args(3)))

    // Wait till job1 completes
    job2.waitForCompletion(true)

    logger.info("Job 2 completed...\n")

    // Exit after completion
    System.exit(if(job2.waitForCompletion(true))  0 else 1)
  }
}
