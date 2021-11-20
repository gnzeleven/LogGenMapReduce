# Log File Processing using Hadoop Map/Reduce

## Introduction

The goal of this project to gain experience with solving a distributed computational problem using cloud computing technologies. Design and implement an instance of the map/reduce computational model for the following four tasks:

<b>1)</b> Compute distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types
<b>2)</b> Compute time intervals sorted in the descending order, that contains most log messages of type ERROR with injected regex pattern string instances
<b>3)</b> Compute the number of the generated log messages for each message type
<b>4)</b> Compute the number of characters in each log message for each type that contain the highest number of characters in the detected instances of the designated regex pattern

## Installation
Tools: IntelliJ IDEA 2021.2.1(Community Edition), jdk 1.8.0_191, Scala 2.13.6, sbt 1.5.2, hadoop 1.2.1

##### To build the project:
* Clone this repository through command line using 
```
> git clone https://github.com/gnzeleven/LogGenMapReduce.git
```
* Open IntelliJ IDEA and navigate File -> Open Folder -> LogGenMapReduce
* To run tests, use the command 
```
> sbt clean compile test
```
* To build, run 
```
> sbt clean compile assembly
``` 
in the command line or 
```
> clean compile assembly
``` 
sequentially in sbt shell
* The above command will create a jar file named <i>LogGenMapReduce-assembly-0.1.jar</i> inside the folder <i>./target/scala-2.13</i>, which means build process went through successfully

##### To run the project:
* Start the Hortonworks Virtualbox VM through VMWare or Virtualbox and SSH into the machine
* Change directory to <i>home/hdfs</i>
* Change the user from <i>root</i> to <i>hdfs</i> by running the command ```su hdfs```
* Copy the input file and the jar file using scp or sftp, for instance if the input file is input.txt and jar file name is ProjectName-assembly-0.1.jar, scp command would be:
```
> scp -P 2222 {path/to/local/file}/input.txt user@hostname:/home/hdfs/{some folder}
> scp -P 2222 {path/to/local/file}/ProjectName-assembly-0.1.jar user@hostname:/home/hdfs/{some folder}
```
<i>Note: After connecting to the AWS EMR through PuTTy or ssh follow all the steps from the above step</i>
* Copy the input file from the regular directory to hdfs' input directory: 
```
> hdfs dfs -copyFromLocal input.txt /input/input.txt
```
* Now all set, run the jar file using the below command 
```
> hadoop jar ProjectName-assembly-0.1.jar {i} /input/input.txt /output/filename{i}
```
where i = {1, 2, 3, 4} and it represents which task to run. To be more specific run, 
```
> hadoop jar LogGenMapReduce-assembly-0.1.jar 1 /input/input.txt /output/task1
> hadoop jar LogGenMapReduce-assembly-0.1.jar 2 /input/input.txt /output/temp /output/task2
> hadoop jar LogGenMapReduce-assembly-0.1.jar 3 /input/input.txt /output/task3
> hadoop jar LogGenMapReduce-assembly-0.1.jar 4 /input/input.txt /output/task4
```
* To check the output, use the below command. The results will be displayed in the command line
```
> hdfs dfs -cat /output/filename{i}/*
```

## Project Structure

There are two main sections: <b>Main</b> and <b>Test</b>

Main has the following Classes:

* <b>Utils</b>
    * <i>CreateLogger.scala</i> - Used for Logging
    * <i>ObtainConfigReference.scala</i> - Used for getting configuration parameters
    * <i>WriteCsv.scala</i> - Convert tab separated csv files to comma separated csv files
* <b>MapReduce</b>
    * <i>MapReduce1.scala</i> - First task: Compute distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types
    * <i>MapReduce2.scala</i> - Second task: Compute time intervals sorted in the descending order, that contains most log messages of type ERROR with injected regex pattern string instances
    * <i>MapReduce3.scala</i> - Third task: Compute the number of the generated log messages for each message type
    * <i>MapReduce4.scala</i> - Fourth task: Compute the number of characters in each log message for each type that contain the highest number of characters in the detected instances of the designated regex pattern
* <b>Driver.scala</b> - Class with the main function. This is where the flow starts. Calls respective task based on command line argument args(0) value

Test has the following class

* <b>MapReduceTestSuite.scala</b> - Contains various test methods to test verify functionalities in the MapReduce jobs such as whether the max function works as intended, time interval check works as expected, pattern matching works like it should for the test input, and whether the Mappers, Reducers, Custom Classes are assigned to the jobs correctly.


## Configuration Settings
Inorder to refrain from using hard coded values in the code, the configuration parameters and provided in src/main/resources/application.conf file and are read in the
scala file ObtainConfigReference helper object. This makes the code reliable and improves the readability of the code. It is generally considered a good practice to read parameters from a .config file. 

<b>MapReduce1</b>
- Start Time: 15:17:40.940
- End Time: 15:18:59.280
- String Pattern: [0-4a-cN-R]+

<b>MapReduce2 and MapReduce4</b>
- String Pattern: [0-4a-cN-R]+

## Documentation

[Click here](https://github.com/gnzeleven/LogGenMapReduce/blob/main/target/scala-2.13/api/index.html) to read the Scala Docs documentation.

YouTube link for video documentation - [here](https://www.youtube.com/watch?v=dQw4w9WgXcQ)

## Input

The input is the log files generated by [this project](https://github.com/0x1DOCD00D/LogFileGenerator). Here are a few examples of the logs look like:

```
15:16:59.970 [scala-execution-context-global-23] INFO  HelperUtils.Parameters$ - HRYFbZN"%<#@f#|?[`@XL\!A#<3Q(P
15:16:59.985 [scala-execution-context-global-23] DEBUG HelperUtils.Parameters$ - /%K|>xd?HFbV#p!(>IIV1)E@[YVX,|we\a(eDFXroAus4`Y}Ab1')_z3<;7$
15:17:00.001 [scala-execution-context-global-23] WARN  HelperUtils.Parameters$ - !,/WBr+eN"'7_PpP6u}Q<xEx;~3h/&ZOzU4>X#-|78&D?+{b'`ZE
15:17:00.017 [scala-execution-context-global-23] DEBUG HelperUtils.Parameters$ - a&9`B,}&DQNFn'e(,u.`m`Jz[*K\W*B\|"k!hxnO
15:17:00.033 [scala-execution-context-global-23] WARN  HelperUtils.Parameters$ - 8w|S]F6&<>|u#Q<}>ar*aSPZ|k_'?a7E~mhFEd[L:Kx,_O<KQjOrV;Q|B;
```

[Click here](https://github.com/gnzeleven/LogGenMapReduce/blob/main/data/test_data_log.txt) to see the sample input log file.

## Implementation

#### Partitioning the data:

A partitioner partitions the key-value pairs of the intermediate outputs from the Mapper. The partitioned can have user-defined logic to assign Map outputs to any of the Reducer tasks. The number of partitions is same as the number of Reducer tasks for the job.

For the tasks 1, 3, and 4, the Map writes Error Type as key and some kind of metric as value. I implemented 2 Reducer tasks for the tasks. The log file is generated in such a way that the message type <i>INFO</i> will be having the highest count (around 65% of the total). Thus, it makes sense to assign all the <i>INFO</i> type messages to one reducer, thus balancing the workloads of the two Reducer tasks to a certain degree.

```
  override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      val errorType = value.toString().split("\t")(0)
      
      if (numReduceTasks == 0) return 0
      if (errorType == "INFO") return 1 % numReduceTasks
      return 0
  }
```

For the task 2, the first Map will write time interval as the key and count of <i>ERROR</i> type as value. Since time interval, as the name implies, can be split into different intervals, I partitioned the intermediate output from Map to be assigned to any of 4 different Reducer tasks depending on the value of seconds in the particular time interval from the Mapper. This balances the load between 4 Reducer tasks.


```
  override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      val interval = value.toString().split("\t")(0)
      val seconds = interval.split(":").last.toInt

      if (numReduceTasks == 0) return 0
      if (seconds >= 45 && seconds < 60) return 1 % numReduceTasks
      else if (seconds >= 30 && seconds < 45) return 2 % numReduceTasks
      else if (seconds >= 15 && seconds < 30) return 3 % numReduceTasks
      return 0
  }
```

#### MapReduce1, MapReduce3, MapReduce4:

The CountMapper in MapReduce1 checks if the injected string pattern(from the config file) exists in the input line and whether the time in the input line is within the interval specified by start and end time from the config file. If the conditions are met, CountMapper writes error type as key and 1 as value. CountMapper in MapReduce3 works the same way, but without the conditions. MaxMapper in MapReduce4 checks for the presence of the injected string pattern from config file in the input message. If yes, it writes error type as key and length of the message as value.

The CustomPartitioner works as specified in the above section. The Combiner(CountReducer here) accepts the inputs from CountMapper and passes the output key-value pairs to the Reducer(also CountReducer). The main function of the Combiner is to summarize the map outputs that have the same key.

The CountReducer gets the input as key-value pairs where key is the key from CountMapper and the value is a Iterable Collection of values summarized for the particular key. The role of Reducer is to aggregate all the values from the collection and returns key-value pair of key and aggregated sum(for MapReduce1 and MapReduce3), maximum length(for MapReduce4).

For more implementation details, refer to the comments in the code.

#### MapReduce2:

MapReduce2 task has 2 Mappers (CountMapper and SwapMapper) and 2 Reducers (CountReducer and SortReducer). A little side note: the time in the generated log file follows HH:MM:SS.sss format and the frequency of the message is in the milliseconds precision. So each second could be counted as an interval. 

The first mapper checks if the error type in the input line is ERROR and if the regex pattern is present. If yes, Mapper will write the interval and 1 as key-value pair. The CustomPartitioner works in the way as described in the above section. The CountReducer aggregates the count of Error Messages in the particular interval.

The output from CountReducer goes as the input for the second mapper i.e., SwapMapper, which as one might guess does some kind of Swapping. It swaps the key-value pair from CountReducer to value-key pair as its key-value pair output. Inception!

The SwapMapper's key-value pair output(which is actually value-key pair from the first reducer) then goes to SortReducer. SortReducer sorts the key-value pair in descending order based on the key(count of error messages). The descending order sort can be achieved by creating a CustomComparator class that extends WritableComparator class. After the sort, the SortReducer writes interval(from the collection of values), count(the key) as key-value. The final output is time interval, number of error messages in the interval sorted in descending order.

## Results: 

The output csv files are located in [this](https://github.com/gnzeleven/LogGenMapReduce/tree/main/output/comma_delimited) directory

#### MapReduce1:
| Error Type | Count  |
| :--------: | :-:    |
| DEBUG      | 323    |
| ERROR      | 34     |
| INFO       | 2350   |
| WARN       | 606    |

#### MapReduce2:

| Interval | Error Message Count  |
| :--------: | :-:    |
| 15:17:29	| 5 |
| 15:18:29	| 3 |
| 15:17:14	| 3 |
15:16:26	| 2 |
15:16:56	| 2 |
15:17:28	| 2 |
15:17:49	| 2 |
15:17:25	| 2 |
15:16:48	| 2 |
15:17:48	| 2 |
15:17:04	| 2 |
15:17:19	| 2 |
15:18:28	| 2 |
15:18:02	| 2 |
15:16:39	| 2 |
15:16:38	| 2 |
15:17:20	| 2 |
15:15:59	| 2 |
15:18:05	| 2 |
15:16:58	| 2 |
15:18:32	| 1 |
15:18:31	| 1 |
15:18:27	| 1 |
15:18:23	| 1 |
15:18:19	| 1 |
15:18:18	| 1 |
15:18:15	| 1 |
15:18:10	| 1 |
15:18:09  | 1 |
15:18:06	| 1 |
15:18:04	| 1 |
15:18:03	| 1 |
15:17:58	| 1 |
15:17:56	| 1 |
15:17:53	| 1 |
15:17:52	| 1 |
15:17:51	| 1 |
15:17:50	| 1 |
15:17:47	| 1 |
15:17:42	| 1 |
15:17:41	| 1 |
15:17:34	| 1 |
15:17:27	| 1 |
15:17:24	| 1 |
15:17:18	| 1 |
15:17:15	| 1 |
15:17:09	| 1 |
15:17:08	| 1 |
15:17:05	| 1 |
15:16:52	| 1 |
15:16:51	| 1 |
15:16:50	| 1 |
15:16:47	| 1 |
15:16:46	| 1 |
15:16:44	| 1 |
15:16:42	| 1 |
15:16:40	| 1 |
15:16:37	| 1 |
15:16:35	| 1 |
15:16:32	| 1 | 
15:16:30	| 1 |
15:16:29	| 1 |
15:16:28	| 1 |
15:16:27	| 1 |
15:16:25	| 1 |
15:16:23	| 1 |
15:16:20	| 1 |
15:16:19	| 1 |
15:16:18	| 1 |
15:16:17	| 1 |
15:16:16	| 1 |
15:16:10	| 1 |
15:16:09	| 1 |
15:16:07	| 1 |
15:16:01	| 1 |
15:17:10	| 1 |


#### MapReduce3:
| Error Type | Count  |
| :--------: | :-:    |
| DEBUG      | 1002   |
| ERROR      | 104    |
| INFO       | 7023   |
| WARN       | 1871   |

#### MapReduce4:
| Error Type | Count  |
| :--------: | :-:    |
| DEBUG      | 91     |
| ERROR      | 90     |
| INFO       | 99     |
| WARN       | 94     |


## Summary

In conclusion, this project involved implementing a working Map/Reduce model in Scala for a semi-realtime data to accomplish various summmarizing tasks. I gained a good intuition about how Mappers and Reducers work in tandem to accomplish parallel processing of data. I learned how to create an executable jar file, basics of Hadoop framework, how to use Hortonworks Sandbox VM, deploying and executing the said jar file in Amazon Web Services' Elasic Map Reduce cluster. Lastly, I thoroughly(as far as I can tell) documented the work in this README.md file.


