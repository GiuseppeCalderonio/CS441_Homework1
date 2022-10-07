import HelperUtils.HelperFunctions.{generate, generateRandomTimestamp}
import HelperUtils.Parameters
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import javax.print.attribute.standard.JobName
import scala.io.Source
import scala.util.Random

/**
 * This test class verifies that the Statistical Map Reducer component works correctly
 * In particular, the approach used is to create custom input files from scratch with pre-computed values
 * (made scalable with the number of time intervals and with the regexp pattern),
 * then run the job, and finally verify if the job output matches with the expected pre-computed values
 *
 * ASSUMPTION: in order to work correctly, the test assumes that the time intervals are correctly configured
 */
class StatisticalMapReducerTest extends AnyFlatSpec with Matchers with PrivateMethodTester {

  behavior of "Statistical Map Reducer"

  // define the inputs of the runJob method
  private val jobName = MapReducers.StatisticalMapReducer.getClass.getName
  private val outputValueClass = classOf[Text]
  private val outputFormatClass = classOf[TextOutputFormat[Text, Text]]
  private val mapperClass = classOf[MapReducers.StatisticalMapReducer.Map]
  private val reducerClass = classOf[MapReducers.StatisticalMapReducer.Reduce]
  private val inputPath = s"log/$jobName"
  private val outputPath = s"log_output/$jobName"

  private val timeIntervals = Parameters.timeIntervals
  private val testTimeIntervals = Parameters.testTimeIntervals
  private val head = timeIntervals.head
  private val messageTypes = Parameters.messageTypes.replace("(", "").replace(")", "").replace("|", ",")

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

  it should "Execute the job and report that for each type of message, three of them match with the regexp pattern" in{

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file with 5 log messages, all in the first time interval, where only 3 of them match with the regexp pattern

    val newInputFile = Array(
      generateRandomTimestamp(head).toString + " [] " + "INFO" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "WARN" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "DEBUG" + " test - " + generate(Parameters.generatingPattern),
      generateRandomTimestamp(head).toString + " [] " + "ERROR" + " test - ",
      generateRandomTimestamp(head).toString + " [] " + "INFO" + " test - "
    ).reduce( (line1, line2) => line1 + "\n" + line2 )

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job

    // this is expected to have 3 lines
    val outputLines = runTestJob().split("\n")

    // verify if the output matches with the expected output

    val output0 = "DEBUG,1," + (0 until timeIntervals.length - 1).map(_ => "0").reduce((s1, s2) => s1 + "," + s2 )
    val output1 = "INFO,1," + (0 until timeIntervals.length - 1).map(_ => "0").reduce( (s1, s2) => s1 + "," + s2 )
    val output2 = "WARN,1," + (0 until timeIntervals.length - 1).map(_ => "0").reduce( (s1, s2) => s1 + "," + s2 )

    val output = Array(output0, output1, output2)

    outputLines should be(output)
  }

  it should "Execute the job and report that for each type of message, three of them match with the time intervals" in {

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file that generates for each time interval two right timestamps and two wrong timestamps

    // this outside generates the strings for each time interval
    val newInputFile = timeIntervals.zip(testTimeIntervals).map( tuple_timeIntervals =>

      // this inside generates the strings for each message type
      messageTypes.split(",")
        .map( msgType => generateRandomTimestamp(tuple_timeIntervals._1).toString + " [] " + msgType + " test - " + generate(Parameters.generatingPattern) + "\n"
         + generateRandomTimestamp(tuple_timeIntervals._2).toString + " [] " + msgType + " test - " + generate(Parameters.generatingPattern))
        .reduce( (s1, s2) => s1 + "\n" + s2 )

     ).reduce((s1, s2) => s1 + "\n" + s2)

    // at the end, for each message type it generates 2n strings, 2 for each time interval, one that should match and one that doesn't

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job

    val outputLines = runTestJob().split("\n")

    // verify if the output matches with the expected output

    val output0 = "DEBUG," + timeIntervals.indices.map(_ => "1").reduce((s1, s2) => s1 + "," + s2 )
    val output1 = "ERROR," + timeIntervals.indices.map(_ => "1").reduce((s1, s2) => s1 + "," + s2 )
    val output2 = "INFO," + timeIntervals.indices.map(_ => "1").reduce((s1, s2) => s1 + "," + s2 )
    val output3 = "WARN," + timeIntervals.indices.map(_ => "1").reduce((s1, s2) => s1 + "," + s2 )

    val output = Array(output0, output1, output2, output3)

    outputLines should be(output)
  }
}
