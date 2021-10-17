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

class MapReduce2

object MapReduce2 {

  val config = ObtainConfigReference("MapReduce2") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  val logger = CreateLogger(classOf[MapReduce2.type])

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val interval = new Text()
    val count = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(' ')
      val stringPattern: Regex = config.getString("MapReduce2.stringPattern").r

      //val time = LocalTime.parse(line(0), formatter)
      val message = line.last
      val messageType = line(2)

      val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
        case Some(_) => true
        case None => false
      }

      logger.info("Pattern present: " + isPatternPresent + " Message: " + message + "\n")

      if (messageType == "ERROR" && isPatternPresent) {
        val time = line(0).toString().split('.')(0)
        interval.set(time)
        context.write(interval, count)
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class SwapperMapper extends Mapper[Object, Text, Text, Text] {

    val count = new Text()
    val interval = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split('\t')
      val k = line(1)
      val v = line(0)
      count.set(k)
      interval.set(v)
      context.write(count, interval)
    }
  }

  class SorterReducer extends Reducer[Text,Text,Text,Text] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      values.asScala.foreach((value) => {
        context.write(value, key)
      })
    }
  }

  def start(args: Array[String]): Unit = {
    val configuration1 = new Configuration
    val job1 = Job.getInstance(configuration1,"Log Gen Map Reduce")
    job1.setJarByClass(classOf[MapReduce2])
    job1.setMapperClass(classOf[TokenizerMapper])
    job1.setCombinerClass(classOf[IntSumReader])
    job1.setReducerClass(classOf[IntSumReader])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(args(1)))
    FileOutputFormat.setOutputPath(job1, new Path(args(2)))
    job1.waitForCompletion(true)

    val configuration2 = new Configuration
    val job2 = Job.getInstance(configuration2,"Log Gen Map Reduce 2")
    job2.setJarByClass(classOf[MapReduce2])
    job2.setMapperClass(classOf[SwapperMapper])
    job2.setCombinerClass(classOf[SorterReducer])
    job2.setReducerClass(classOf[SorterReducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job2, new Path(args(2)))
    FileOutputFormat.setOutputPath(job2, new Path(args(3)))
  }
}
