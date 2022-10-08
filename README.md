# CS-441 HOMEWORK 1

* First Name : Giuseppe
* Last name : Calderonio
* UIC email : gcalde22@uic.edu
* UIN: 679346611
* deployment video on youtube : [https://youtu.be/xn4M0JpCU54](https://youtu.be/xn4M0JpCU54)

## Introduction
This repository contains the implementation of a map reduce
program implemented with the Hadoop framework (Book option 1), and specifically 
implemented for the first homework of the CS-441 Cloud Computing 
class of University of Illinois at Chicago

## Requirements

In order to correctly download and use the project, it's required 
to have the following tools installed :

1. Hadoop v 3.2.1 or greater
2. Scala 3.0.2 or greater
3. an AWS account

## Problem statement

The problem consists in implementing 4 map reducer tasks that
analyze log files, testing 
them locally, and then deploy them on AWS ElasticMapReducer.
More details can be found following [this link](https://github.com/GiuseppeCalderonio/CS441_Fall2022/blob/main/Homeworks/Homework1.md)

## Project description

### Configuration parameters

A [configuration file](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/application.conf)
has been used to assign values to the parameters of the project
that may change frequently over time such as
the type of messages, the number of mappers and reducers,
the time intervals, the regex pattern, and so on...
Then, using the library TypeSafe listed in the [build.sbt fle](https://github.com/GiuseppeCalderonio/Homework1/blob/master/build.sbt)
a class called [Parameters](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/scala/HelperUtils/Parameters.scala)
that takes those values from the config file,
parses them and exposes them as the public interface of
the class.

Note that there are some constraints on the
usage of the configuration file in order to 
let the project work correctly, for example
to list the time intervals in such a way
that they are disjoint intervals, and that
they get merged in a single list correctly (
since in the config file before being parsed
they are two separate
lists of strings
)

### Sharding

In order to split the input log files in shards,
the library logback was used.
Even if in the project is not present the 
LogFileGenerator module (a module to automatically
generate log files), the [logback.xml](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/logback.xml)
file that is
used in that project is reported; it indeed
divides the files in shards, where each shard size 
is specified by the ```<maxFileSize>``` tag.
Specifically, in the [video](https://youtu.be/xn4M0JpCU54)
the input file of 10,000 lines was divided in 12 files
of 100 KB each.

### Inputs

The input of the program can be any file, indeed there is no 
specific requirement about the file format, since each mapper,
before starting executing the function, filters each
line that does not match a log message structure.

However, this can increase the overhead time of the jobs, especially
when there is a big workload : this is indeed a limitation.

The input directory should be passed when the application is run.

### Outputs

The output of the program is a directory, that contains other 4 
subDirectories, each one associated with a job.

The output directory should be passed when the application is run.

### Jobs

For the realization of the homework, 4 jobs (2 
4 mappers and 4 reducers) were implemented.

Each one of them, takes as input a directory containing
the log files to process, and outputs a 
directory for each job (for a total of 4 directories)
, where the content is the output of the reducer(s).

In order to run a job, assuming that there is
a suitable input file and that the project was
correctly downloaded, it's necessary to run 
the following command from the project root :
```
sbt clean compile "run <inputFile> <outputDir>"
```

Where the _<inputFile>_ specifies the relative /
absolute path of the input file, and the 
_<outputDir>_ specifies the output directory where
the aggregated directories will be stored.
It is not necessary to delete the output directory everytime,
since the program overwrites it.

#### First task

The first task description is : _compute a spreadsheet or an
CSV file that shows the distribution of different types of 
messages across predefined time intervals and injected 
string instances of the designated regex pattern for these
log message types_

Where the regex pattern is specified in the [configuration file](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/application.conf)
as Pattern.

The class that implements this functionality is [StatisticalMapReducer.scala](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/scala/MapReducers/StatisticalMapReducer.scala)
, that takes from the configuration parameter
a set of precomputed time intervals specified in the [configuration file](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/application.conf)
and outputs _n+1_ columns, where the _i-th_ column of the
csv file represents the number of log messages that
match the regex pattern **AND** that belong to the 
_i-th_ time interval, while the first column specifies
the type of the message (ERROR, INFO, DEBUG, WARN).

For example, if the input log file is :
```
19:29:33.794 [] DEBUG test - pattern 1
12:42:20.240 [] INFO test - pattern 2
03:22:50.923 [] WARN test - not matches
```

and the timeInterval configuration parameter is 
```
[(00:00:00.000, 06:00:00.000) ,
 (08:00:00.001, 12:00:00.000) ,
 (12:00:00.001, 18:00:00.000),
 (18:00:00.001, 23:59:59.999) ] 
```

and the regex pattern is 
```
 "pattern"
```
Then the output will be in the form :

```
INFO,0,0,0,1
DEBUG,0,0,1,0
```

because the timestamp of the DEBUG message
belongs to the fourth timeInterval and the 
timestamp of the INFO message belongs to the third
, while the WARN message does not match with the 
regex pattern, so it is not considered

#### Second task

The second task description is : _compute time
intervals sorted in the descending order that
contained most log messages of the type ERROR
with injected regex pattern string instances_

The class that implements this functionality is 
[ErrorTimeIntervalsMapReducer](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/scala/MapReducers/ErrorTimeIntervalsMapReducer.scala)
, that as the last task, takes from the configuration parameter
a set of precomputed time intervals specified in the [configuration file](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/application.conf)
and outputs n rows where the _i-th_ row specifies 
the time interval with the number of error messages
that match the regex pattern **AND** that belong to that
time interval.

This task may be interpreted in 2 ways :
1. sorting the lines for **Time intervals**
2. sorting the lines for **Message occurrences**

For the project, the first one was interpreted as
the correct one.



For example, if

```
19:29:33.794 [] ERROR test - pattern 1
12:42:20.240 [] ERROR test - pattern 2
00:00:32.946 [] ERROR test - not matches
```

and the timeInterval configuration parameter is
```
[(00:00:00.000, 06:00:00.000) ,
 (08:00:00.001, 12:00:00.000) ,
 (12:00:00.001, 18:00:00.000),
 (18:00:00.001, 23:59:59.999) ] 
```

and the regex pattern is
```
 "pattern"
```

Then the output will have the following format
```
 [12:00:00.001; 18:00:00.000],1
 [18:00:00.001; 23:59:59.999],1
```

Differently from the previous case, if a time
interval does not have any error message
which time interval both belongs to it and
the message matches with the regex pattern, 
then it won't be printed in the output file at all
(an alternative solution was possible printing 0)

Note that the time intervals are 
sorted by construction since the
mapper job sends to reducers key-value pairs
sorted by key, and since the key for this mapper
job is the time interval represented as an 
Hadoop Text (equivalent to a string),
the time intervals are inherently sorted
(comparison between ```"10:99:99.999"``` and 
```"20:00:00.000"``` returns that the second one is greater
, even though they are both strings and not time
intervals) 

#### Third task

The third task description is : _produce
the number of the generated log messages 
for each message type_

The class that implements this functionality is [TypeCounterMapReducer](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/scala/MapReducers/TypeCounterMapReducer.scala)
, that takes the set of all possible message types from 
the [configuration file](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/application.conf)
and outputs a row for each message type, where 
each row is in the form _TYPE, n_, where n
represents the number of occurrences of this
message type in the input file.

For example, if the input file is
```
19:29:33.794 [] DEBUG test - pattern 1
12:42:20.240 [] INFO test - pattern 2
19:27:01.228 [] ERROR test - pattern 3
13:37:31.395 [] DEBUG test - pattern 4
18:37:31.395 [] INFO test - pattern 5
```

and the regex pattern is
```
 "pattern"
```

then the output file (assuming only one mapper)
will look like

```
DEBUG, 2
ERROR, 1
INFO, 2
WARN, 0 
```

since there are 2 DEBUG messages, 1 ERROR message,
2 INFO messages, and no WARN messages in the input file


#### Fourth task

The fourth task description is : _produce the
number of characters in each log message for 
each log message type that contain the highest
number of characters in the detected instances 
of the designated regex pattern_

The class that implements this functionality is 
[MaxCharCountMapReduce](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/scala/MapReducers/MaxCharCountMapReduce.scala)
, takes the set of all possible message types from
the [configuration file](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/resources/application.conf)
and outputs a row for each message type, where 
each row is in the form _TYPE,n_ , where TYPE is
the type of message, and n is the number of characters
of the message of type TYPE that has the maximum 
number of characters. If there are
no log messages of a certain type, the output file
won't show a row at all for that message type.

For example, if the input file is :
```
19:29:33.794 [] DEBUG test - pattern a
12:42:20.240 [] DEBUG test - pattern aa
19:27:01.228 [] DEBUG test - pattern aaaaaaa
13:37:31.395 [] DEBUG test - pattern aa
18:37:31.395 [] DEBUG test - pattern aaa
```

and the regex pattern is
```
 "pattern"
```

The output file will be

```
DEBUG, 41
```

because the third message has the maximum number of characters
among all the messages of type DEBUG, and it has
exactly 41 characters, while there are no other type
of messages in the input file.

## Testing

In order to test the application, [four test 
classes](https://github.com/GiuseppeCalderonio/Homework1/tree/master/src/test/scala)
are provided, where each test class
is associated with a job.

Each test has the same structure :
1. Delete the input test file, if it exists
2. create a new custom input file, containing the target lines to test
3. execute the _runJob_ function (more on class [MapReducerJob](https://github.com/GiuseppeCalderonio/Homework1/blob/master/src/main/scala/MapReducers/MapReducerJob.scala))
with the test option on (it allows to return the content of the file as a string)
4. verifies if the expected output matches with the 
actual one

Moreover, all the test cases are parametric with
the configuration parameters, meaning
that if they change (ex: regex pattern, time intervals)
, the test should run with a high degree of confidence (some exceptions
are possible, for example if the regex pattern accepts every line)

Two configuration parameters are used only for testing
purposes (_testStartingTimeIntervals_,_testEndingTimeIntervals_)

To run the test, go to the root directory of the
project and run the following command :

```
sbt clean compile test
```

## Deployment

### Jar file

In order to do a local deployment, the
**Assembly** sbt plugin has been used, that allows
to resolve dependencies and create a single jar from
the big project.

In order to create the jar file go to the root
of the project and run the following command :
```
sbt clean compile assembly
```
Then go to the directory target/scala-3.0.2/
and the jar file should have been produced there.

### Local

To run it locally on a custom input file(s) use the 
following command :
```
hadoop jar <jarFileName> <inputDirectory> <outputDirectory>
```

Where the _<jarFileName>_ is the jar file produced,
_<inputDirectory>_ is the path to the directory 
where input files should be,
_<outputDirectory>_ is the directory where aggregated
output data will be stored.

### AWS Elastic Map Reducer

In order to deploy the jar file on the cloud,
there are 3 steps required.

#### Create an s3 bucket

This step consist on creating a cloud repository
of our files. 

In particular, log in on AWS, search for s3, 
and click _Create bucket_, give it a name (i.e. TEST).
Then upload there the jar file previously created
and the input directory containing log files.

#### Run an EMR cluster

This step consists on actually run the jar
file on the cloud in a distributed environment.

In order to do that, search on AWS for EMR, click
create cluster, click on _Go to advanced options_, 
go to _Software Configuration_, select _emr-5.36.0_
select only _Hadoop 2.10.1_ among the possible
tool choices, then in _Steps (optional)_ 
click the checkbox _Cluster auto-terminates_ 
of the option _After last step completes:_
(this avoids to have additional charges),
the select _Custom JAR_ on _Step type_, 
click _Add Step_, select the jar from your
s3 bucket (i.e. s3://TEST/custom.jar), 
put these arguments for the jar file

```
s3://<YOUR-BUCKET-NAME>/<INPUT-DIR> s3://<YOUR-BUCKET-NAME>/<OUTPUT-DIR>
```

click _Add_ 
and then select all the default configurations


#### See the resulting outputs

After 5-10 minutes of deployment, in your s3 bucket
the aggregated results should be visible.

### EMR From intellij

The deployment part can be also managed 
from intellij using the [Big Data Tool](https://www.jetbrains.com/help/idea/big-data-tools-support.html)

More info can be found in this [youtube video](https://youtu.be/xn4M0JpCU54)

## Limitations

The main limitations of the project are :

* Since there are no assumptions on the input file, an additional
 filtering step at the beginning of each mapper is introduced, however
 this implies a time overhead
* Some assumptions are made on the configuration parameters, such as that
 the time intervals need to be sorted and correctly configured
  (each string at the _i-th_ position of the _startTimeInterval_
 config parameter should be less than the
 string at the _i-th_ position of the _endingTimeInterval_
  config parameter)
 , or that the testing time intervals should not overlap among 
 each other (also these ones may have been automatically created)
* Once the jar is created and a configuration parameter changes,
 instead of changing just the content of a config file, the 
 jar needs to be created from scratch
* The format of the output is not on _.csv_, and there is
 no first row description of the output format
* In order to keep mappers and reducers in the same object class,
 some code replication may be present because same reducers were 
 implemented more than once
