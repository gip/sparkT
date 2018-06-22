package sparkt.ast.protocol

import sparkt.ast.database._
import sparkt.ast.sql._
import sparkt.ast.etl._

abstract class APPhrase
case class PSQLSelectStatement(id: Long, execute: Boolean, sql: ASSelect) extends APPhrase
case class PETLStatement(id: Long, execute: Boolean, dag: SETL) extends APPhrase
case class PPing(id: Long, msg: String) extends APPhrase

abstract class APResponse
case class PPong(id: Long, msg: String) extends APResponse
case class PSQLResult(id: Long, response: Either[String, Seq[Seq[String]]]) extends APResponse
case class PUnparseable(msg: Option[String]) extends APResponse
case class PUnsupported(id: Long, msg: String) extends APResponse
case class PETLResult(id: Long, response: Either[String, String]) extends APResponse


object Protocol {
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  val imports = "import sparkt.ast.protocol._; "+
                "import sparkt.ast.sql._; "+
                "import sparkt.ast.database._; "+
                "import sparkt.ast.etl._; "
  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
  def read(str: String): Option[APPhrase] = {
    try {
      val tree = tb.parse(imports + str)
      Some(tb.eval(tree).asInstanceOf[APPhrase])
    } catch {
      case NotImplemented(msg) =>
        println("Couldn't parse expression: Not implemented : " + msg)
        None
      case UnhandledType(msg) =>
        println("Couldn't parse expression: Unhandled type : " + msg)
        None
      case e : java.lang.reflect.InvocationTargetException =>
        println("Evaluation exception: " + e.getCause())
        None
      case e : Throwable =>
        println(e)
        println("Couldn't parse expression: Unexpected exception : " + e.getMessage())
        None
    }
  }

  // TODO: escape strings!
  def show(resp: APResponse): String =
    resp match {
      case PPong(id, msg) => "Pong " + id.toString() + " \"" + msg + "\""
      case PSQLResult(id, Left(msg)) => "SQLResult " + id + " (Left \"" + msg + "\")"
      case PSQLResult(id, Right(msg)) => "SQLResult " + id + " (Right \"" + msg + "\")"
      case PETLResult(id, Left(msg)) => "ETLResult " + id + " (Left \"" + msg + "\")"
      case PETLResult(id, Right(msg)) => "ETLResult " + id + " (Right \"" + msg + "\")"
      case PUnparseable(None) => "Unparseable Nothing"
      case PUnparseable(Some(msg)) => "Unparseable (Just \"" + msg + "\")"
      case PUnsupported(id, msg) => "Unsupported " + id + " \"" + msg + "\""
      case _ => "Unsupported 0 \"Message type\""
    }
}

object Test {
  import Protocol._

  val a = read("""PPing(897, "msg")""")
  val b = show(PSQLResult(456, Left("failed")))
}
