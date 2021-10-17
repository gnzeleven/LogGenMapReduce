package com.cs441.anand.MapReduce

import com.cs441.anand.Utils.ObtainConfigReference
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import scala.collection.JavaConverters._

class MapReduce3

object MapReduce3 {

  val config = ObtainConfigReference("MapReduce3") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val errorType = new Text()
    val count = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(config.getString("MapReduce3.separator"))
      // Add the country (line[2]) to the variable state
      errorType.set(line(2))
      // Write (key: Text, value: IntWritable(count)) to the context
      context.write(errorType, count)
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
    job.setJarByClass(classOf[MapReduce3])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))
  }
}
