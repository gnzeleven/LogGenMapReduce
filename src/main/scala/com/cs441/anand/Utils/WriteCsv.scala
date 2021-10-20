package com.cs441.anand.Utils

import java.io.{BufferedWriter, FileWriter, File}

// Read tab separated csv file and write comma separated csv file
class WriteCsv

/** Factory for [[WriteCsv]] instances */
object WriteCsv {
  val base = System.getProperty("user.dir")
  List(
       "mapreduce1_final.csv",
       "mapreduce2_temp.csv",
       "mapreduce2_final.csv",
       "mapreduce3_final.csv",
       "mapreduce4_final.csv"
  ).foreach((file : String) => writeCommaSeparated(file))

  /** Override reduce function
   * @param filename : name of csv file
   * @return Unit - Writes (interval, count) sorted descending
   */
  def writeCommaSeparated(filename: String) : Unit = {
    // get the path
    val path = base + "/output/"
    val outputFile = new File(path + "comma_delimited/" + filename)

    // create bufferedSource and bufferedWrite
    val bufferedSource = io.Source.fromFile(path + "tab_delimited/" + filename)
    val bufferedWriter = new BufferedWriter(new FileWriter(outputFile))

    // for each line in bufferedSource, add comma and write bufferedWriter
    bufferedSource.getLines.foreach((line : String) => {
      val cols = line.split("\t").map(_.trim)
      val row = cols(0) + ", " + cols(1) + "\n"
      bufferedWriter.write(row)
    })

    // close bufferedSource and bufferedWrite
    bufferedSource.close()
    bufferedWriter.close()
  }
}
