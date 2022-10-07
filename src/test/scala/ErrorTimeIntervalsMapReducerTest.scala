import HelperUtils.HelperFunctions.{areSortedTimeIntervals, generate, generateRandomTimestamp}
import HelperUtils.Parameters
import MapReducers.ErrorTimeIntervalsMapReducer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.time.LocalTime

/**
 * This test class verifies that the Error Time Map Reducer component works correctly
 *
 */
class ErrorTimeIntervalsMapReducerTest extends AnyFlatSpec with Matchers with PrivateMethodTester{


  behavior of "Error time intervals map reducer test"

  // define the inputs of the runJob method
  private val jobName = MapReducers.ErrorTimeIntervalsMapReducer.getClass.getName
  private val outputValueClass = classOf[IntWritable]
  private val outputFormatClass = classOf[TextOutputFormat[Text, IntWritable]]
  private val mapperClass = classOf[MapReducers.ErrorTimeIntervalsMapReducer.Map]
  private val reducerClass = classOf[MapReducers.ErrorTimeIntervalsMapReducer.Reduce]
  private val inputPath = s"log/$jobName"
  private val outputPath = s"log_output/$jobName"

  private val timeIntervals = Parameters.timeIntervals
  private val testTimeIntervals = Parameters.testTimeIntervals
  private val head = timeIntervals.head

  def runTestJob(): String =
    MapReducers.MapReducerJob.runJob(
      jobName = jobName,
      outputValueClass = outputValueClass,
      outputFormatClass = outputFormatClass,
      mapperClass = mapperClass,
      reducerClass = reducerClass,
      inputPath = inputPath,
      outputPath = outputPath + "TEST",
      nMappers = "1",
      nReducers = "1",
      isTest = true
    )

  it should "Output an empty string when injecting messages that are not of ERROR type" in {

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file with 3 log messages, without even an error one

    val newInputFile = Array(
      generateRandomTimestamp(head).toString + " [] " + "INFO" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "WARN" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "DEBUG" + " test - " + generate(Parameters.generatingPattern)
    ).reduce((line1, line2) => line1 + "\n" + line2)

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job, and verifies if it throws an exception due to the empty reducer

    val emptyString = runTestJob()

    emptyString.isEmpty should be(true)

  }

  it should "Output the occurrence of error messages with strings that match the regexp pattern" in {

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file with 4 log error messages, two that match with the specified regexp pattern, and 2 no

    val newInputFile = Array(
      generateRandomTimestamp(head).toString + " [] " + "ERROR" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "ERROR" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "ERROR" + " test - ",
      generateRandomTimestamp(head).toString + " [] " + "ERROR" + " test - "
    ).reduce((line1, line2) => line1 + "\n" + line2)

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job

    val outputLines = runTestJob().split("\n")

    // verify if the output matches with the expected output

    val output = Array("", "[ " + timeIntervals.head._1 + " ; " + timeIntervals.head._2 + " ] ,2")

    outputLines should be(output)

  }

  it should "Output the occurrences of error messages with different time intervals and verifies that those are sorted in descending order" in {

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file with tree messages for each time interval all of type error,
    // three that belong to time intervals but one without regexp pattern, and one that does not belong to time intervals

    val newInputFile = timeIntervals.zip(testTimeIntervals)
      .map(tuple_timeIntervals =>
        generateRandomTimestamp(tuple_timeIntervals._1).toString + " [] ERROR test - " + generate(Parameters.generatingPattern) + "\n"
        + generateRandomTimestamp(tuple_timeIntervals._1).toString + " [] ERROR test - " + generate(Parameters.generatingPattern) + "\n"
        + generateRandomTimestamp(tuple_timeIntervals._2).toString + " [] ERROR test - " + generate(Parameters.generatingPattern) + "\n"
        + generateRandomTimestamp(tuple_timeIntervals._1).toString + " [] ERROR test - " )
      .reduce((s1, s2) => s1 + "\n" + s2)

    // at the end, for each time interval it generates 4 strings, two that matches and one that doesn't

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job

    val outputLines = runTestJob().split("\n")

    // verify if the output matches with the expected output

    val output = Array("").concat(timeIntervals.map( t =>  "[ " + t._1.toString + " ; " + t._2.toString + " ] ,2"  ).toArray)

    areSortedTimeIntervals(timeIntervals) should be(true)

    outputLines should be(output)

  }




}
