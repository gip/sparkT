package sparkt.backend.services

import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Flow

import sparkt.ast.protocol._
import sparkt.backend.compiler._
import sparkt.backend.spark._
import sparkt.backend.etl._

object SparkTService {
  def route: Route = path("sparkt" / "v1") {
    get {
      handleWebsocketMessages(sparkTServiceV1)
    }
  }

  val sparkTServiceV1: Flow[Message, Message, _] = Flow[Message].map {
    case TextMessage.Strict(txt) =>
      val resp = Protocol.read(txt) match {
        case None => PUnparseable(Some(txt))
        case Some(PPing(id, msg)) => PPong(id, msg)
        case Some(PSQLSelectStatement(id, exe, select)) =>
          println("Received SQL")
          if (exe) {
            val df = Context.compiler.compileSelect(select)
            PSQLResult(id, Left(df.take(10).toString()))
          } else {
            PSQLResult(id, Left("SQL valid"))
          }
        case Some(PETLStatement(id, exe, etl)) =>
          println("Received ETL -> " + etl.identifier)
          if (exe) {
            val r = ETL.execute(etl)
            PETLResult(id, r)
          } else {
            PETLResult(id, Left("ETL valid"))
          }
        case _ => PUnsupported(0, "Unsupported message type")
      }
      TextMessage(Protocol.show(resp))
    case _ => TextMessage("Message type unsupported")
  }
}
