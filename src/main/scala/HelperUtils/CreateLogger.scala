package HelperUtils

import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * this object is used to create a logger
 *
 * The implementation follows the same structure of the CreateLogger class of the LogGeneration project
 */
object CreateLogger:

  /**
   * This constructor creates the logger object that can be then used to log messages
   * @param class4Logger this parameter represents the class type of the logger
   * @tparam T this parameter represents the type of the class that creates the logger itself i.e. class A{ val l = CreateLogger(classOf[A.type]) }
   * @return the logger created
   */
  def apply[T](class4Logger: Class[T]):Logger =
    val LogbackXML = "logback.xml"
    val logger = LoggerFactory.getLogger(class4Logger)
    Try(getClass.getClassLoader.getResourceAsStream(LogbackXML)) match {
      case Failure(exception) => logger.error(s"Failed to locate $LogbackXML for reason $exception")
      case Success(inStream) => inStream.close()
    }
    logger