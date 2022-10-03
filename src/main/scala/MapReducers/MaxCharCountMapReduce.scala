package MapReducers

import HelperUtils.HelperFunctions.filterLogMessagesOnly
import HelperUtils.Parameters
import MapReducers.MapReducerJob.runJob
import com.google.gson.*
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.IOException
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex


/**
 * This object implements the fourth functionality : produce the number
 * of characters in each log message for each log message type that
 * contain the highest number of characters in the detected instances of the designated regex pattern
 *
 * MAPPER : it divides the sharp in lines, filter them for regexp, and collect the
 *          message type as the KEY and the message length as the VALUE
 * REDUCER : it takes the maximum among the set of collected length of the message types,
 *           and collects the message type as the KEY and the maximum length as the VALUE
 */
object MaxCharCountMapReduce :

  /**
   * it divides the sharp in lines, filter them for regexp, and collect the
   * message type as the KEY and the message length as the VALUE
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

    private val pattern = new Regex(Parameters.generatingPattern)
    private val messageTypes = new Regex(Parameters.messageTypes)


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      val count = new IntWritable()


      filterLogMessagesOnly(value)
        .filterNot(pattern.findFirstIn(_).isEmpty) // filter for regexp
        .map(line => ( // tuple
          messageTypes.findFirstIn(line).get, // type of message
          line.length)) // number of chars
        .foreach{ msg_count =>
          count.set(msg_count._2)
          output.collect(new Text(msg_count._1), count)
        }


  /**
   * it takes the maximum among the set of collected length of the message types,
   * and collects the message type as the KEY and the maximum length as the VALUE
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

    private val max = new IntWritable()

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      
      max.set(values.asScala.map(str => str.get()).max) // take the maximum value
      output.collect(key, max)

  /**
   * runs the job
   *
   * @param inputPath  the input path of the job
   * @param outputPath the output path of the job
   * @return the output of the job as a string
   */
  def run(inputPath : String, outputPath : String): String =

    // first line of the csv file to show
    val firstLine = "Message Type, number of char"
    val jobName = this.getClass.getName.replace("$", "")

    runJob(jobName,
      classOf[IntWritable],
      classOf[TextOutputFormat[Text, IntWritable]],
      classOf[Map],
      classOf[Reduce],
      firstLine = firstLine,
      inputPath = inputPath,
      outputPath = s"$outputPath/$jobName.csv"
    )

