package HelperUtils

import com.typesafe.config.Config

import java.time.LocalTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/**
 * This module obtains configuration parameter values from application.conf and converts them
 * into appropriate scala types.
 *
 * The implementation follows the same structure of the Parameters class of the LogGeneration project
 *
 */
object Parameters :

  /**
   * This value is used to locate the configuration name at the root of the .config file
   */
  private val configName = "Homework1Config"

  /**
   * this value is used for logging purposes
   */
  private val logger = CreateLogger(classOf[Parameters.type])

  /**
   * this value represents the object used to access the .config file
   */
  private val config = ObtainConfigReference("Homework1Config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  /**
   * this function takes two lists of time intervals represented as strings (start) and (end) timestamps
   * and returns a single list of time intervals (start, end) represented as a pair of LocalTime objects
   * if the
   * @param startTimeIntervalsStrings list of strings in "HH:mm:ss.SSS" format representing start time intervals
   * @param endTimeIntervalsStrings list of strings "HH:mm:ss.SSS" format representing end time intervals
   * @throws IllegalArgumentException if the format specified is not "HH:mm:ss.SSS", or if a starting interval is higher than an ending interval
   * @return a list of time intervals (start, end) represented as a pair of LocalTime objects
   */
  private def timeIntervals(startTimeIntervalsStrings: List[String], endTimeIntervalsStrings: List[String]): List[(LocalTime, LocalTime)] =

    if startTimeIntervalsStrings.length != endTimeIntervalsStrings.length then {
      logger.error(s"number of elements of the two lists are not equal, they should be equal")
      throw new IllegalArgumentException("Incorrect setting of time intervals : number of elements of the two lists are not equal, they should be equal")
    }

    try{
      val startTimeIntervals = startTimeIntervalsStrings.map(LocalTime.parse(_, DateTimeFormatter.ofPattern("HH:mm:ss.SSS")))
      val endTimeIntervals = endTimeIntervalsStrings.map(LocalTime.parse(_, DateTimeFormatter.ofPattern("HH:mm:ss.SSS")))

      val intervals = startTimeIntervals.zip(endTimeIntervals)
      if intervals.exists(t => t._1.isAfter(t._2)) then {
        logger.error(s"start interval must be lower than end interval")
        throw new IllegalArgumentException("Incorrect setting of time intervals : start interval must be lower than end interval")
      }
      intervals

    } catch {
      case _ : DateTimeParseException => logger.error(s"format should be \"HH:mm:ss.SSS\"")
        throw new IllegalArgumentException("Incorrect setting of time intervals : format should be \"HH:mm:ss.SSS\"")
    }

  end timeIntervals


  /**
   * this function returns a list of strings given a parameter name
   * in particular, it searches for the parameter name in the .config file, and if it does not find it, it
   * throws an exception
   * @param stringListName this parameter represents the name of the configuration parameter that should be a list of strings
   * @throws IllegalArgumentException if the parameter name does not exists in the .config file of it is not a list
   * @return
   */
  private def getStringListSafe(stringListName : String): List[String] =
    Try(config.getStringList(s"$configName.$stringListName").asScala.toList) match {
      case Success(value) => value
      case Failure(_) => logger.error(s"No config parameter $stringListName is provided")
        throw new IllegalArgumentException(s"No config data for $stringListName")
    }
  end getStringListSafe

  /**
   * This function returns a list of time intervals represented as pairs of LocalTime objects given the
   * parameter names of the list of starting time intervals and ending time intervals that are
   * in the .config file represented as list of strings
   * it calls the methods "getStringListSafe" and "timeIntervals" to accomplish this task
   *
   * @param startingTimeIntervalsConfig this parameter represents the parameter name on the .config file of the
   *                                    starting time interval list of strings
   * @param endingTimeIntervalsConfig this parameter represents the parameter name on the .config file of the
   *                                  ending time interval list of strings
   * @return a list of time intervals sorted and represented as a pair of LocalTime Objects
   */
  private def getTimeIntervals(startingTimeIntervalsConfig : String, endingTimeIntervalsConfig : String): List[(LocalTime, LocalTime)] =

    val startingTimeIntervals = getStringListSafe(startingTimeIntervalsConfig)
    val endingTimeIntervals = getStringListSafe(endingTimeIntervalsConfig)

    timeIntervals(startingTimeIntervals, endingTimeIntervals)

  end getTimeIntervals

  /**
   * this function returns the parameter value always represented as a string
   * (because the application uses only strings) corresponding to the name "pName"
   * in the .config file if exists, otherwise the default value "defaultVal" is chosen
   *
   * @param pName the name of the .config parameter string to get
   * @param defaultVal the default value of the parameter if it does not exists in the .config file
   * @return the value of the parameter in the .config file associated with the name "pName" if exists, "defaultVal" otherwise
   */
  private def getParam(pName: String, defaultVal: String): String =

    Try(config.getString(s"$configName.$pName")) match {
      case Success(value) => value
      case Failure(_) => logger.warn(s"No config parameter $pName is provided. Defaulting to $defaultVal")
        defaultVal
    }
  end getParam


  /**
   * these values represent the public interface of the object Parameters
   * the description of each of them can be found in the .config file (should be located in the src/main/resources folder)
   * these values can't be changed once the jar is created
   */

  val generatingPattern: String = getParam("Pattern", "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
  val messageTypes: String = getParam("messageTypes","(INFO|WARN|ERROR|DEBUG)")
  val timeRegexp: String = getParam("timeRegexp", "([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{3})")
  val tempDir: String = getParam("tempDir", "temp1/")
  val nMappers: String = getParam("nMappers", "1")
  val nReducers: String = getParam("nReducers", "1")
  val timeIntervals: List[(LocalTime, LocalTime)] = getTimeIntervals("startingTimeIntervals", "endingTimeIntervals")
  val testTimeIntervals: List[(LocalTime, LocalTime)] = getTimeIntervals("testStartingTimeIntervals", "testEndingTimeIntervals")

