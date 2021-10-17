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

class MapReduce4

object MapReduce4 {

  val config = ObtainConfigReference("MapReduce4") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  val logger = CreateLogger(classOf[MapReduce4.type])

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val errorType = new Text()
    val length = new IntWritable()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(' ')
      logger.info("************" + config.getString("MapReduce4.stringPattern"))
      val stringPattern: Regex = config.getString("MapReduce4.stringPattern").r

      val message = line.last

      val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
        case Some(_) => true
        case None => false
      }

      logger.info("Pattern present: " + isPatternPresent + " Message: " + message + "\n")

      if (isPatternPresent) {
        errorType.set(line(2))
        length.set(message.length)
        context.write(errorType, length)
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    val result = new IntWritable()
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
//      val max = values.asScala.foldLeft((0))((max, x) => {
//        if(max > x.get) max
//        else x.get
//      })
      val max = values.asScala.foldLeft(0)(_ max _.get)
      result.set(max)
      context.write(key, result)
    }
  }

  def start(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"Log Gen Map Reduce")
    job.setJarByClass(classOf[MapReduce4])
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
