package sparkt.ast.sql

case class SDatabaseInfo(s: String)
case class STableSchema(s: String)

case class NotImplemented(msg: String) extends Throwable

// Untyped SQL AST
// TODO: generate the classes from the Haskell AST
abstract class ASCommand
case class SSelectCommand(select: ASSelect) extends ASCommand
case class SInsertCommand(select: ASInsert) extends ASCommand

abstract class ASSelect
case class SSelect(table: ASSelectType,
                   ordering: Seq[ASOrdering],
                   limit: Option[Int],
                   offset: Option[Int]) extends ASSelect

abstract class ASInsert
case class SInsert(info: Option[SDatabaseInfo],
                   name: String,
                   schema: Option[STableSchema],
                   fields: Seq[String],
                   values: ASInsertValues) extends ASInsert

abstract class ASInsertValues
case class SInsertValues(values: Seq[Seq[ASExpr]]) extends ASInsertValues
case class SInsertSelect(select: ASSelect) extends ASInsertValues

abstract class ASSelectType
case class SSelectTable(proj: Seq[(ASExpr, Option[String])],
                        from: Option[ASFrom],
                        where: Option[ASExpr],
                        grouping: Option[ASGrouping],
                        having: Option[ASExpr]) extends ASSelectType

abstract class ASFrom
case class SFromTable(source: ASTableSource, name: Option[String]) extends ASFrom

abstract class ASTableSource
case class STableNamed(name: String, schema: Any) extends ASTableSource
case class STableSubSelect(subSelect: ASSelect) extends ASTableSource

abstract class ASOrdering
case class SAsc(expr: ASExpr) extends ASOrdering
case class SDesc(expr: ASExpr) extends ASOrdering

abstract class ASGrouping

abstract class ASExpr
case class SLitString(lit: String) extends ASExpr
case class SLitInt(lit: Int) extends ASExpr
case class SLitDouble(lit: Int) extends ASExpr
case class SBinop(op: String, lhs: ASExpr, rhs: ASExpr) extends ASExpr

object Parser {
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
  def parse[T](str: String): Option[T] = {
    try {
      val tree = tb.parse("import sparkt.ast._; "+ str)
      Some(tb.eval(tree).asInstanceOf[T])
    } catch {
      case NotImplemented(msg) =>
        println("Couldn't parse expression: Not implemented : " + msg)
        None
      case e : Throwable =>
        println("Couldn't parse expression: Unexpected exception : " + e.getMessage())
        None
    }
  }
}

object Test {
  import Parser._
  val select = """SSelectCommand(SSelect(SSelectTable(Seq(),Some(SFromTable(STableNamed("t0",None),Some("t1"))),None,None,None),Seq(),None,None))"""
  val insert = """SInsertCommand(SInsert(None,"theTable",None,Seq("a","b","c"),SInsertSelect(SSelect(SSelectTable(Seq(),Some(SFromTable(STableNamed("t0",None),Some("t1"))),None,None,None),Seq(),None,None))))"""

  val selectAst = parse[ASCommand](select)
  val insertAst = parse[ASCommand](insert)
}
