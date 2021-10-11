package com.cs441.anand

import com.cs441.anand.MapReduce.{MapReduce1, MapReduce3}
import com.cs441.anand.Utils.CreateLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.Logger

class Driver

object Driver {

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"Log Gen Map Reduce")
    val logger = CreateLogger(classOf[Driver.type])
    logger.info("Map Reduce model is starting...")

    args(0) match {
      case "1" => setMapReduce1(job)
      case "2" => setMapReduce2(job)
      case "3" => setMapReduce3(job)
      case "4" => setMapReduce4(job)
      case _ => logAndExit(logger)
    }

    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))
    logger.info("Map Reduce model finished...")
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

  def setMapReduce1(job: Job): Unit = {
    job.setJarByClass(classOf[MapReduce1])
    job.setMapperClass(classOf[MapReduce1.TokenizerMapper])
    job.setCombinerClass(classOf[MapReduce1.IntSumReader])
    job.setReducerClass(classOf[MapReduce1.IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
  }

  def setMapReduce2(job: Job): Unit = {
    job.setJarByClass(classOf[MapReduce3])
    job.setMapperClass(classOf[MapReduce3.TokenizerMapper])
    job.setCombinerClass(classOf[MapReduce3.IntSumReader])
    job.setReducerClass(classOf[MapReduce3.IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
  }

  def setMapReduce3(job: Job): Unit = {
    job.setJarByClass(classOf[MapReduce3])
    job.setMapperClass(classOf[MapReduce3.TokenizerMapper])
    job.setCombinerClass(classOf[MapReduce3.IntSumReader])
    job.setReducerClass(classOf[MapReduce3.IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
  }

  def setMapReduce4(job: Job): Unit = {
    job.setJarByClass(classOf[MapReduce3])
    job.setMapperClass(classOf[MapReduce3.TokenizerMapper])
    job.setCombinerClass(classOf[MapReduce3.IntSumReader])
    job.setReducerClass(classOf[MapReduce3.IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
  }

  def logAndExit(logger: Logger): Unit = {
    logger.info("INVALID job - Set 1 or 2 or 3 or 4.")
    System.exit(0);
  }
}
