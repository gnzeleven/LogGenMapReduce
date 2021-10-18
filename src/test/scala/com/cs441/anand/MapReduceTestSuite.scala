package com.cs441.anand

import com.cs441.anand.MapReduce.{MapReduce1, MapReduce2, MapReduce3, MapReduce4}
import com.cs441.anand.Utils.ObtainConfigReference
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

class MapReduceTestSuite extends AnyFlatSpec with Matchers {
  /*
    MapReduce1
   */
  val config1 = ObtainConfigReference("MapReduce1") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // assert given time is between start and end
  it should "check if given time should be in between start and end" in {
    val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
    val start = LocalTime.parse(config1.getString("MapReduce1.startTime"), formatter)
    val end = LocalTime.parse(config1.getString("MapReduce1.endTime"), formatter)
    // should return true
    val time_true = LocalTime.parse("15:18:11.111", formatter)
    // should return false
    val time_false = LocalTime.parse("15:16:04.111", formatter)
    assert((start.isBefore(time_true) && end.isAfter(time_true)) &&
          !(start.isBefore(time_false) && end.isAfter(time_false)))
  }

  // assert mapper class
  it should "check if the mapper class is correctly assigned in the job" in {
    val configuration1 = new Configuration
    val job1 = Job.getInstance(configuration1,"test MapReduce1")
    job1.setMapperClass(classOf[MapReduce1.CountMapper])
    assert(job1.getMapperClass() == classOf[MapReduce1.CountMapper])
  }

  /*
    MapReduce2
   */
  val config2 = ObtainConfigReference("MapReduce2") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // assert custom comparator class
  it should "check if the custom comparator class is correctly assigned in the job" in {
    val configuration2 = new Configuration
    val job2 = Job.getInstance(configuration2,"test MapReduce2")
    job2.setSortComparatorClass(classOf[MapReduce2.CustomComparator])
    assert(job2.getSortComparator.isInstanceOf[MapReduce2.CustomComparator])
  }

  // assert if the regex pattern is present
  it should "check if regex pattern is present in the given input string" in {
    val value = "15:16:07.068 [scala-execution-context-global-23] ERROR HelperUtils.Parameters$ - c=aWM.Jae2R8oW6rag0D8qce3&C91>rJ"
    val line = value.split(' ')
    val stringPattern: Regex = config2.getString("MapReduce2.stringPattern").r
    val message = line.last
    val messageType = line(2)
    val isPatternPresent = stringPattern.findFirstMatchIn(message) match {
      case Some(_) => true
      case None => false
    }
    assert(messageType == "ERROR" && isPatternPresent)
  }

  /*
    MapReduce3
   */

  // assert reducer class
  it should "check if the reducer class is correctly assigned in the job" in {
    val configuration3 = new Configuration
    val job3 = Job.getInstance(configuration3,"test MapReduce3")
    job3.setReducerClass(classOf[MapReduce3.CountReducer])
    assert(job3.getReducerClass() == classOf[MapReduce3.CountReducer])
  }

  // check output key and value class
  it should "check if the output classes are correctly assigned in the job" in {
    val configuration3 = new Configuration
    val job3 = Job.getInstance(configuration3,"test MapReduce3")
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])
    assert(job3.getOutputKeyClass == classOf[Text]
      && job3.getOutputValueClass == classOf[IntWritable])
  }

  /*
    MapReduce4
  */
  val config4 = ObtainConfigReference("MapReduce4") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // check for max value
  it should "check if max value is computed" in {
    val values = List(10, 88, 1024, 1111, 555)
    val max = values.foldLeft(0)(_ max _)
    assert(max == 1111)
  }

  // assert combiner class
  it should "check if the combiner class is correctly assigned in the job" in {
    val configuration4 = new Configuration
    val job4 = Job.getInstance(configuration4,"test MapReduce4")
    job4.setCombinerClass(classOf[MapReduce4.MaxReducer])
    assert(job4.getCombinerClass() == classOf[MapReduce4.MaxReducer])
  }
}
