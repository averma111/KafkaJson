package org.ashish.impl

import org.slf4j.{Logger, LoggerFactory}
import scala.language.implicitConversions

trait Logging {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit def logging2Logger(anything: Logging): Logger = anything.logger

}
