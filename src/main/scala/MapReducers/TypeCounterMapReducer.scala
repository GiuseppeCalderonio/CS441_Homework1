package MapReducers

import HelperUtils.HelperFunctions.filterLogMessagesOnly
import HelperUtils.Parameters
import MapReducers.ErrorTimeIntervalsMapReducer.{Map, Reduce}
import MapReducers.MapReducerJob.runJob
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
import scala.util.matching.Regex


/**
 * this object represents the third functionality :  produce the number of the generated log messages
 * MAPPER : it divides the sharp in lines, extracts the message type and collect the
 *          message type as the KEY, and the constant 1 as the VALUE
 * REDUCER : it sums all the aggregated values, so counting the number of log messages for type,
 *           and then outputs the message type as the KEY, and the sum as the VALUE
 */
object TypeCounterMapReducer :

  /**
   * it divides the sharp in lines, extracts the message type and collect the
   * message type as the KEY, and the constant 1 as the VALUE
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private val messageType = new Text()
    private val one = new IntWritable(1)
    private val messageTypes = new Regex(Parameters.messageTypes)
    
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      filterLogMessagesOnly(value)
        .foreach{ line =>
          messageType.set(messageTypes.findFirstIn(line).get)
          output.collect(messageType, one)
        }


  /**
   * it sums all the aggregated values, so counting the number of log messages for type,
   * and then outputs the message type as the KEY, and the sum as the VALUE
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

    private val sum = new IntWritable()

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =


      sum.set( values.asScala.map(int => int.get()).sum )// sum all the types of messages previously collected
      output.collect(key, sum)

  @main def runTypeCounterMapReducer: String =

    // first line of the csv file to show
    val firstLine = "Message type, number of occurrences"

    runJob(this.getClass.getName,
      classOf[IntWritable],
      classOf[TextOutputFormat[Text, IntWritable]],
      classOf[Map],
      classOf[Reduce],
      firstLine = firstLine
    )

