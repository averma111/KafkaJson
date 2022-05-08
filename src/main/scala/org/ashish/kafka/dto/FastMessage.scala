package org.ashish.kafka.dto

import play.api.libs.functional.syntax.{toFunctionalBuilderOps, unlift}
import play.api.libs.json.{JsPath, Json, Writes}

object FastMessageJsonImplicits  {
  implicit val fastMessageFmt: Any = Json.format[FastMessage]
  //implicit val fastMessageWrites: Any = Json.writes[FastMessage]
  implicit val fastMessageReads: Any = Json.reads[FastMessage]
  implicit val fastMessageWrites: Writes[FastMessage] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "eventid").write[String]
    )(unlift(FastMessage.unapply))
}

case class FastMessage(name: String, randamnumber: String)

