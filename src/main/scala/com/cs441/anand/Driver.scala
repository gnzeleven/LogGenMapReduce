package com.cs441.anand

import com.cs441.anand.MapReduce.{MapReduce1, MapReduce2, MapReduce3, MapReduce4}
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
      case "1" => MapReduce1.start(args)
      case "2" => MapReduce2.start(args)
      case "3" => MapReduce3.start(args)
      case "4" => MapReduce4.start(args)
      case _ => logAndExit(logger)
    }
    logger.info("Map Reduce model finished...")
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

  def logAndExit(logger: Logger): Unit = {
    logger.info("INVALID job - Set 1 or 2 or 3 or 4.")
    System.exit(0);
  }
}
