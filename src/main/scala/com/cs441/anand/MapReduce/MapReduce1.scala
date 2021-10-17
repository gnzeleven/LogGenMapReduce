package com.cs441.anand.MapReduce

import com.cs441.anand.Utils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.util.matching.Regex

class MapReduce1

object MapReduce1 {

  val config = ObtainConfigReference("MapReduce1") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  val logger = CreateLogger(classOf[MapReduce1.type])

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val errorType = new Text()
    val count = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(' ')
      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val startTime = LocalTime.parse(config.getString("MapReduce1.startTime"), formatter)
      val endTime = LocalTime.parse(config.getString("MapReduce1.endTime"), formatter)
      val stringPattern: Regex = config.getString("MapReduce1.stringPattern").r

      val time = LocalTime.parse(line(0), formatter)
      val message = line.last

      val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
        case Some(_) => true
        case None => false
      }

      logger.info("Pattern present: " + isPatternPresent + " Message: " + message + "\n")
      logger.info("Before: " + startTime.isBefore(time) + "\n")
      logger.info("After: " + endTime.isAfter(time) + "\n")

      if (startTime.isBefore(time) && endTime.isAfter(time) && isPatternPresent) {
        errorType.set(line(2))
        context.write(errorType, count)
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  def start(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"Log Gen Map Reduce")
    job.setJarByClass(classOf[MapReduce1])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))
  }
}
