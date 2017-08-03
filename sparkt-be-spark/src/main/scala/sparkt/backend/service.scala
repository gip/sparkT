package sparkt.backend.services

import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Flow

import sparkt.ast.protocol._

object SparkTService {
  def route: Route = path("sparkt" / "v1") {
    get {
      handleWebsocketMessages(sparkTServiceV1)
    }
  }

  val sparkTServiceV1: Flow[Message, Message, _] = Flow[Message].map {
    case TextMessage.Strict(txt) =>
      println(txt)
      val resp = Protocol.read(txt) match {
        case None => PUnparseable(Some(txt))
        case Some(PPing(id, msg)) => PPong(id, msg)
        case Some(PSQLStatement(id, exe, sql)) => PUnsupported(id, "SQLStatement")
        case Some(PETLStatement(id, exe, dag)) => PUnsupported(id, "ETLStatement")
        case _ => PUnsupported(0, "Unsupported message type")
      }
      TextMessage(Protocol.show(resp))
    case _ => TextMessage("Message type unsupported")
  }
}
