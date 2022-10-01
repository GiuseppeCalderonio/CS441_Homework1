
package MapReducers

import HelperUtils.Parameters
import HelperUtils.HelperFunctions.filterLogMessagesOnly
import MapReducers.MapReducerJob.runJob
import MapReducers.StatisticalMapReducer.{Map, Reduce}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.time.format.DateTimeFormatter
import java.time.LocalTime
import com.google.gson.*

import scala.jdk.CollectionConverters.*
import java.io.IOException
import java.util
import scala.language.postfixOps
import scala.util.matching.Regex


/**
 * This object implements the second functionality : compute time intervals
 * sorted in the descending order that contained most log messages of the
 * type ERROR with injected regex pattern string instances
 *
 *  MAPPER : it divides the sharp in lines, filters error messages only, filters for strings
 *           that match the regexp pattern, finds whether the message timestamp belongs to
 *           one of the time interval, and eventually sets it as the KEY of the mapper,
 *           and sets the constant 1 as the VALUE if a time interval is found
 *  REDUCER : it sums all the aggregated values, so counting the number of time interval occurrences,
 *            and then outputs the time interval as the KEY, and the sum as the VALUE
 *
 *  Note that the intervals are automatically sorted by the mapper
 */
object ErrorTimeIntervalsMapReducer :

  /**
   * it divides the sharp in lines, filters error messages only, filters for strings
   * that match the regexp pattern, finds whether the message timestamp belongs to
   * one of the time interval, and eventually sets it as the KEY of the mapper,
   * and sets the constant 1 as the VALUE if a time interval is found
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private val one = new IntWritable(1)
    private val stringTimeInterval = new Text()
    private val timeIntervals = Parameters.timeIntervals
    private val timeRegexp = new Regex(Parameters.timeRegexp)
    private val pattern = new Regex(Parameters.generatingPattern)
    private val messageTypes = new Regex(Parameters.messageTypes)



    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      def belongsTo(timestamp: String, timeInterval : (LocalTime, LocalTime)): Boolean = {
        val p = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

        if (LocalTime.parse(timestamp, p).isAfter(timeInterval._1) &&
          !LocalTime.parse(timestamp, p).isAfter(timeInterval._2)) {
          true
        }else false

      }

      filterLogMessagesOnly(value)
        .filter(messageTypes.findFirstIn(_).get.matches("ERROR")) // Filter for error messages only
        .filterNot(pattern.findFirstIn(_).isEmpty) // filter for regexp
        .foreach{ line =>

          timeIntervals.filter(belongsTo(timeRegexp.findFirstIn(line).get, _))
            .foreach( (start, end) =>
              stringTimeInterval.set( "[ " + start.toString + " ; " + end.toString + " ] ")
              output.collect(stringTimeInterval, one)
            )
        }

  /**
   * it sums all the aggregated values, so counting the number of time interval occurrences,
   * and then outputs the time interval as the KEY, and the sum as the VALUE
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

    private val sum = new IntWritable()

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      sum.set(values.asScala.map(num => num.get()).sum)
      output.collect(key, sum)



  @main def runErrorTimeIntervalsMapReducer: String =

    // first line of the csv file to show
    val firstLine = "Time Interval, error messages"

    runJob(ErrorTimeIntervalsMapReducer.getClass.getName,
      classOf[IntWritable],
      classOf[TextOutputFormat[Text, IntWritable]],
      classOf[Map],
      classOf[Reduce],
      firstLine = firstLine
    )
