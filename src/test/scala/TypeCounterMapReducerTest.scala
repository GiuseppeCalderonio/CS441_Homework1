import HelperUtils.HelperFunctions.{generate, generateRandomTimestamp}
import HelperUtils.Parameters
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}

/**
 * This test class verifies that the Type Counter Map Reducer component works correctly
 *
 */
class TypeCounterMapReducerTest extends AnyFlatSpec with Matchers with PrivateMethodTester{

  behavior of "Type counter map reducer test"

  // define the inputs of the runJob method
  private val jobName = MapReducers.TypeCounterMapReducer.getClass.getName
  private val outputValueClass = classOf[IntWritable]
  private val outputFormatClass = classOf[TextOutputFormat[Text, IntWritable]]
  private val mapperClass = classOf[MapReducers.TypeCounterMapReducer.Map]
  private val reducerClass = classOf[MapReducers.TypeCounterMapReducer.Reduce]
  private val inputPath = s"log/$jobName"
  private val outputPath = s"log_output/$jobName"
  private val tempOutputPath = jobName

  private val timeIntervals = Parameters.timeIntervals
  private val testTimeIntervals = Parameters.testTimeIntervals
  private val messageTypes = Parameters.messageTypes.replace("(", "").replace(")", "").replace("|", ",")

  it should "Count the number of generated error messages regardless of the time interval and pattern regexp matching" in {

    // delete, if exists, the input file

    HelperUtils.HelperFunctions.deleteRecursively(new File(inputPath))

    // create a new input file that contains all kind of combinations between belonging to time intervals and pattern regexp

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

    // this should generate 4n messages, n for each message type, where n is the size of the time intervals

    val pw = new PrintWriter(new File(inputPath))
    pw.write(newInputFile)
    pw.close()

    // execute the job

    val outputLines = MapReducers.MapReducerJob.runJob(
      jobName = jobName,
      outputValueClass = outputValueClass,
      outputFormatClass = outputFormatClass,
      mapperClass = mapperClass,
      reducerClass = reducerClass,
      inputPath = inputPath,
      outputPath = outputPath,
      tempOutputPath = tempOutputPath
    ).split("\n")

    // verify if the output matches with the expected output

    val output0 = "DEBUG," + (timeIntervals.length * 4).toString
    val output1 = "ERROR," + (timeIntervals.length * 4).toString
    val output2 = "INFO," + (timeIntervals.length * 4).toString
    val output3 = "WARN," + (timeIntervals.length * 4).toString

    val output = Array("", output0, output1, output2, output3)

    outputLines should be(output)

  }

}
