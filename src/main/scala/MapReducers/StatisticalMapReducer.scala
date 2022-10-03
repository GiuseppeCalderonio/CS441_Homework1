package MapReducers

import HelperUtils.HelperFunctions.filterLogMessagesOnly
import HelperUtils.Parameters
import MapReducers.MapReducerJob.runJob
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.{DataInput, DataOutput, IOException}
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

/**
 * This object implements the first functionality :  compute a spreadsheet
 * or an CSV file that shows the distribution of different types of messages
 * across predefined time intervals and injected string instances of the
 * designated regex pattern for these log message types
 *
 *    MAPPER : divides the sharp in lines, filters them if they match the regexp pattern,
 *             identifies the type of message (the output KEY of the mapper),
 *             for each time interval computes 1 if the message timestamp belongs to it, 0 otherwise and
 *             groups them in an array with the one-hot encoding pattern
 *             i.e. if timestamp = 3.5 and timeInterval = [ (1, 2) , (2, 3) , (3, 4) , (4, 5) ] the resulting
 *             array will be in the form ohTimeInterval = [   0    ,   0    ,   1    ,   0    ],
 *             and collects this array (the output VALUE of the mapper) in a custom parsed text format
 *   REDUCER : reduces the arrays passed as input value (as text format, and then translated bak to arrays),
 *             summing the values on the same index
 *             i.e. v1=[0, 1, 3, 0]
 *                  v2=[1, 2, 0, 5]
 *                  v3=[0, 5, 2, 9]
 *                vTot=[1, 8, 5, 14]
 *             , and then returns the message type as the KEY, and the reducer array as the VALUE
 */
object StatisticalMapReducer :

  /**
   * this attribute is used for parsing the array in text and back
   */
  private val separator = ","

  /**
   * this function is used to parse an array of integers in a text string format
   * the parsing is executed dividing each value of the array with a separator
   * i.e. writeT( [1, 3, 4] ) = "1\3\4" when separator = "\"
   * @param timestampCount the integer array to parse
   * @return the parsed string of the array
   */
  def writeT(timestampCount: Array[Int]): Text =
    new Text(timestampCount.map(_.toString).reduce((s1, s2) => s1 + separator + s2))

  /**
   * this function parses back the array from its text input format
   * i.e. writeT( "1\3\4" ) = [1, 3, 4] when separator = "\"
   * If the values passed are not consistent, an exception is thrown
   * @param text the text to parse back
   * @return the array parsed back from the string
   */
  def readT(text: Text): Array[Int] =
    text.toString.split(separator).map(_.toInt)


  /**
   * divides the sharp in lines, filters them if they match the regexp pattern,
   * identifies the type of message (the output KEY of the mapper),
   * for each time interval computes 1 if the message timestamp belongs to it, 0 otherwise and
   * groups them in an array with the one-hot encoding pattern
   * i.e. if timestamp = 3.5 and timeInterval = [ (1, 2) , (2, 3) , (3, 4) , (4, 5) ] the resulting
   * array will be in the form ohTimeInterval = [   0    ,   0    ,   1    ,   0    ],
   * and collects this array (the output VALUE of the mapper) in a custom parsed text format
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] :
    private val messageType = new Text()
    private val timeIntervals = Parameters.timeIntervals
    private val timeRegexp = new Regex(Parameters.timeRegexp)
    private val pattern = new Regex(Parameters.generatingPattern)
    private val messageTypes = new Regex(Parameters.messageTypes)


    def belongsTo(timestamp: String, timeInterval: (LocalTime, LocalTime)): Int = {
      val p = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

      if (LocalTime.parse(timestamp, p).isAfter(timeInterval._1) &&
        !LocalTime.parse(timestamp, p).isAfter(timeInterval._2)) {
        1
      } else 0

    }


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =

      filterLogMessagesOnly(value)
         .filterNot(pattern.findFirstIn(_).isEmpty) // filter for regexp pattern
         .foreach{ line =>
          messageType.set(messageTypes.findFirstIn(line).get) // get the type of the message
          val timestampCount = timeIntervals.map(belongsTo(timeRegexp.findFirstIn(line).get, _)).toArray // get the array representing if the message belongs to a time interval in one-hot format
          output.collect(messageType,
            writeT(timestampCount)
          )
        }


  /**
   * reduces the arrays passed as input value (as text format, and then translated bak to arrays),
   * summing the values on the same index
   * i.e. v1=[0, 1, 3, 0]
   * v2=[1, 2, 0, 5]
   * v3=[0, 5, 2, 9]
   * vTot=[1, 8, 5, 14]
   * ,and then returns the message type as the KEY, and the reducer array as the VALUE
   */
  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, Text] :

    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit =

      val sum = values.asScala
        .map(readT)
        .reduce( (v1, v2) => v1.zip(v2).map( int => int._1 + int._2) )


      output.collect(key, writeT(sum))


  /**
   * runs the job
   *
   * @param inputPath  the input path of the job
   * @param outputPath the output path of the job
   * @return the output of the job as a string
   */
  def run(inputPath : String, outputPath : String): String =

    // first line of the csv file to show
    val firstLine = "MessageType" + "," + Parameters.timeIntervals.map( t => "belonging to [" + t._1 + ";" + t._2 + "]").reduce( (s1, s2)  => s1 + "," + s2 )
    val jobName = this.getClass.getName.replace("$", "")

    runJob(jobName,
      classOf[Text],
      classOf[TextOutputFormat[Text, Text]],
      classOf[Map],
      classOf[Reduce],
      firstLine = firstLine,
      inputPath = inputPath,
      outputPath = s"$outputPath/$jobName.csv"
    )
