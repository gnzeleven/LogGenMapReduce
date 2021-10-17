package com.cs441.anand

import com.cs441.anand.MapReduce.{MapReduce1, MapReduce2, MapReduce3, MapReduce4}
import com.cs441.anand.Utils.CreateLogger
import org.slf4j.Logger

class Driver

/** Factory for [[Driver]] instances */
object Driver {

  /** Main Method - Triggers the MapReduce method based on command line input
   * @param args : Array[String] - command line input
   * @return Unit
   */
  def main(args: Array[String]): Unit = {
    val logger = CreateLogger(classOf[Driver.type])
    logger.info("Map Reduce model is starting...")

    // switch which MapReduce to start based on args(0)
    // "1" - MapReduce1
    // "2" - MapReduce2
    // "3" - MapReduce3
    // "4" - MapReduce4
    // default - exit
    args(0) match {
      case "1" => MapReduce1.start(args)
      case "2" => MapReduce2.start(args)
      case "3" => MapReduce3.start(args)
      case "4" => MapReduce4.start(args)
      case _ => logAndExit(logger)
    }
  }

  /** Method that gets triggered if args(0) is invalid
   * @param logger : Logger object
   * @return Unit
   */
  def logAndExit(logger: Logger): Unit = {
    logger.info("INVALID job - Set 1 or 2 or 3 or 4.")
    System.exit(0);
  }
}
