package com.cs441.anand.MapReduce

import com.cs441.anand.Utils.ObtainConfigReference
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import java.lang.Iterable
import scala.collection.JavaConverters._

class MapReduce3

object MapReduce3 {

  val config = ObtainConfigReference("MapReduce3") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val messageType = new Text()
    val count = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(config.getString("MapReduce3.separator"))
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
  }
}
