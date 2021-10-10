name := "LogGenMapReduce"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
