package HelperUtils

import com.mifmif.common.regex.Generex
import dk.brics.automaton.{RegExp, State, Transition}
import org.apache.hadoop.io.Text

import java.io.File
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.util.Random
import scala.util.matching.Regex

/**
 * This object represents a set of functions, mainly used to generate random strings matching a regexp or
 * belonging to a time interval
 */
object HelperFunctions {

        private val timeRegexp = new Regex(Parameters.timeRegexp)
        private val messageTypes = new Regex(Parameters.messageTypes)


        /**
         * this function returns a string given a timestamp
         * @param timestamp the timestamp to convert
         * @return the string from the timestamp
         */
        def getStringFromTimestamp(timestamp: LocalTime): String =
                val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
                timestamp.format(formatter);

        /**
         * This method deletes the directory "file" recursively
         *
         * @param file the file / directory to delete recursively
         */
        def deleteRecursively(file: File): Unit =
                if (file.isDirectory) {
                        file.listFiles.foreach(deleteRecursively)
                }
                if (file.exists && !file.delete) {
                        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
                }
        end deleteRecursively


        /**
         * this function creates a directory
         *
         * @param file the path of the directory to create
         */
        def createDirectory(file: String): Unit =

                val dir = new File(file)
                dir.mkdir()
        end createDirectory


        /**
         * function that extends a string from size 'n' to size 'n + size' with zeroes at the beginning of it
         * example : extend('aaa', 5) = '00000aaa'
         *
         * @param str  the string to extend
         * @param size the extension factor
         * @return the extended strings
         */
        def extend(str: String, size: Int): String =
                if str.length < size then return extend("0" + str, size)
                str

        /**
         * this function verifies if a set of log lines are sorted for time intervals
         * @param outLines this array of strings represents the log lines
         * @return true if the lines are sorted by timestamp, false otherwise
         */
        def verifyTimeIntervals(outLines : Array[String]): Boolean =
                val p = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
                val timeInterval = outLines
                  .filterNot(s => s.isEmpty)
                  .map( line => (
                    LocalTime.parse(timeRegexp.findAllIn(line).toList.head, p),
                    LocalTime.parse(timeRegexp.findAllIn(line).toList(1), p)
                    )).toList
                areSortedTimeIntervals(timeInterval)
        /**
         * function that verifies if a set of time intervals is sorted
         * for example , [ ( 1, 2) , (3, 4) ] is because 2 is less than 3,
         *              [ (1, 2) , (1.5, 6) ] is not because 2 is greater than 1.5
         * @param timeIntervals the set of time intervals to verify
         * @return true if the time intervals are sorted, false otherwise
         */
        private def areSortedTimeIntervals(timeIntervals: List[(LocalTime, LocalTime)]): Boolean =
                (0 until timeIntervals.length - 1)
                  .map(i => timeIntervals(i)._2.isBefore(timeIntervals(i + 1)._1))
                  .reduce((b1, b2) => b1 && b2)

        /**
         * this function computes a random timestamp belonging to the open range specified by the input parameter
         *
         * @param timeInterval this parameter represents the open range in which the random timestamp should belong to
         * @return a random timestamp belonging to the open range timeInterval
         */
        def generateRandomTimestamp(timeInterval: (LocalTime, LocalTime)): LocalTime =

                val startTimestamp = timeInterval._1.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")).replace(".", ":").split(":").map(_.toInt)
                val endTimestamp = timeInterval._2.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")).replace(".", ":").split(":").map(_.toInt)

                val difference = startTimestamp.zip(endTimestamp).map(start_end => start_end._2 - start_end._1)

                val firstPositiveValue = difference.find(int => int > 0) match
                        case Some(value) => difference.indexOf(value)
                        case None => difference.length - 1

                val length = startTimestamp.length

                val maxValues = Array(23, 59, 59, 999)
                val extendFactor = Array(2, 2, 2, 3)

                val intString = (0 until length).toArray
                  .map(int => {
                          if int < firstPositiveValue then startTimestamp(int)
                          else if int == firstPositiveValue then Random.between(startTimestamp(int), endTimestamp(int))
                          else Random.between(startTimestamp(int), maxValues(int))
                  })
                  .map(_.toString)

                val timestampString = (0 until length)
                  .map(i => extend(intString(i), extendFactor(i)))
                  .reduce((str1, str2) => str1 + str2)
                val formatted = timestampString.substring(0, 2) + ":" + timestampString.substring(2, 4) + ":" + timestampString.substring(4, 6) + "." + timestampString.substring(6)

                LocalTime.parse(formatted, DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))


        /**
         * Generates a random String based on the given regular expression
         *
         * @param regex the regexp that the output string will match
         * @return a random string matching the regexp pattern
         */
        def generate(regex: String): String = new Generex(regex).random()

        /**
         * this function is used to filter a text file embedded in the value text String in an array of strings
         * where each element is a line of the file, and it also needs to be a log message line
         * if it is not, the line is filtered out by the function
         * @param value the text string representing the content of the file to filter
         * @return an array of log messages belonging to the value string
         */
        def filterLogMessagesOnly(value : Text): Array[String]  =
        value.toString.split("\n") // divide the text in lines
          .filterNot(timeRegexp.findFirstIn(_).isEmpty)
          .filterNot(messageTypes.findFirstIn(_).isEmpty)

}