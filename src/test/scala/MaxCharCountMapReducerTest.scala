import HelperUtils.HelperFunctions.{generate, generateRandomTimestamp}
import HelperUtils.Parameters
import MapReducers.MaxCharCountMapReduce
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.time.LocalTime

/**
 * This test class verifies that the Max Char Count Map Reducer component works correctly
 *
 */
class MaxCharCountMapReducerTest extends AnyFlatSpec with Matchers with PrivateMethodTester{

  behavior of "Max Char Count Map Reducer Test"

  // define the inputs of the runJob method
  private val jobName = MapReducers.MaxCharCountMapReduce.getClass.getName
  private val outputValueClass = classOf[IntWritable]
  private val outputFormatClass = classOf[TextOutputFormat[Text, IntWritable]]
  private val mapperClass = classOf[MapReducers.MaxCharCountMapReduce.Map]
  private val reducerClass = classOf[MapReducers.MaxCharCountMapReduce.Reduce]
  private val inputPath = s"log/$jobName"
  private val outputPath = s"log_output/$jobName"

  private val timeIntervals = Parameters.timeIntervals
  private val testTimeIntervals = Parameters.testTimeIntervals
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

  it should "Output the number of characters of the message with maximum characters for each message type log regardless of the time intervals and pattern regexp matching" in{

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file that generates for each time interval two right timestamps and two wrong timestamps

    // this outside generates the strings for each time interval
    val newInputFile = timeIntervals.zip(testTimeIntervals).map(tuple_timeIntervals =>

      // this inside generates the strings for each message type, both for strings that match the pattern regexp and not
      messageTypes.split(",")
        .map(msgType => generateRandomTimestamp(tuple_timeIntervals._1).toString + " [] " + msgType + " test - " + generate(Parameters.generatingPattern) + "\n"
          + generateRandomTimestamp(tuple_timeIntervals._2).toString + " [] " + msgType + " test - " + generate(Parameters.generatingPattern) + "\n"
          + generateRandomTimestamp(tuple_timeIntervals._1).toString + " [] " + msgType + " test - " + "\n"
          + generateRandomTimestamp(tuple_timeIntervals._2).toString + " [] " + msgType + " test - ")
        .reduce((s1, s2) => s1 + "\n" + s2)

    ).reduce((s1, s2) => s1 + "\n" + s2)

    // now get the maximum number of char for each message type

    val maxChars = messageTypes.replace("(", "").replace(")", "").replace("|", ",").split(",")
      .map( msgType => newInputFile.split("\n").filter( line => line.contains(msgType)).map( line => line.length ).max )

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job

    val outputLines = runTestJob().split("\n")

    // verify if the output matches with the expected output

    val output = messageTypes.split(",").zip(maxChars).map( m_c => m_c._1 + "," + m_c._2.toString )

    outputLines should be(output)

  }

}
