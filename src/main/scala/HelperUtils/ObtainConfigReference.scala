
package HelperUtils

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

/**
 * this object is used to get a reference that gives access to the .config file
 *
 * The implementation follows the same structure of the ObtainConfigReference class of the LogGeneration project
 */
object ObtainConfigReference:
  private val config = ConfigFactory.load()
  private def ValidateConfig(confEntry: String):Boolean = Try(config.getConfig(confEntry)) match {
    case Failure(exception) => System.err.println(s"Failed to retrieve config entry $confEntry for reason $exception"); false
    case Success(_) => true
  }

  def apply(confEntry:String): Option[Config] = if ValidateConfig(confEntry) then Some(config) else None
