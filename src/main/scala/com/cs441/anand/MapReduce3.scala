package com.cs441.anand

import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConverters._

class MapReduce3

object MapReduce3 {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val messageType = new Text()
    val count = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(' ')
      // Add the country (line[2]) to the variable state
      messageType.set(line(2))
      // Write (key: Text, value: IntWritable(count)) to the context
      context.write(messageType, count)
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration
//    val job = Job.getInstance(configuration,"word count")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass(classOf[TokenizerMapper])
//    job.setCombinerClass(classOf[IntSumReader])
//    job.setReducerClass(classOf[IntSumReader])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputKeyClass(classOf[Text]);
//    job.setOutputValueClass(classOf[IntWritable]);
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }
}
