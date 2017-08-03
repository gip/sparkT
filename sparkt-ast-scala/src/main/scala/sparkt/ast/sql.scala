package sparkt.ast.sql

import sparkt.ast.database.DDatabaseMapping

case class NotImplemented(msg: String) extends Throwable
case class UnhandledType(msg: String) extends Throwable

// Untyped SQL AST
// TODO: generate the classes from the Haskell AST
abstract class ASCommand
case class SSelectCommand(select: ASSelect) extends ASCommand
case class SInsertCommand(insert: ASInsert) extends ASCommand

abstract class ASSelect
case class SSelect(table: ASSelectType,
                   ordering: Seq[ASOrdering],
                   limit: Option[Int],
                   offset: Option[Int]) extends ASSelect

abstract class ASInsert
case class SInsert(info: Option[DDatabaseMapping],
                   name: String,
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
case class SInnerJoin(lhs: ASFrom, rhs: ASFrom, on: Option[ASExpr]) extends ASFrom
case class SOuterJoin(lhs: ASFrom, rhs: ASFrom, on: Option[ASExpr]) extends ASFrom
case class SLeftJoin(lhs: ASFrom, rhs: ASFrom, on: Option[ASExpr]) extends ASFrom
case class SRightJoin(lhs: ASFrom, rhs: ASFrom, on: Option[ASExpr]) extends ASFrom

abstract class ASTableSource
case class STableNamed(info: Option[DDatabaseMapping], name: String) extends ASTableSource
case class STableSubSelect(subSelect: ASSelect) extends ASTableSource

abstract class ASOrdering
case class SAsc(expr: ASExpr) extends ASOrdering
case class SDesc(expr: ASExpr) extends ASOrdering

abstract class ASGrouping
case class SGrouping(groups: Seq[ASExpr]) extends ASGrouping

abstract class ASSetQuantifier
case class SSetQuantifierAll() extends ASSetQuantifier
case class SSetQuantifierDistinct() extends ASSetQuantifier

abstract class ASExpr
case class SLitString(lit: String) extends ASExpr
case class SLitInt(lit: Int) extends ASExpr
case class SLitDouble(lit: Double) extends ASExpr
case class SBinop(op: String, lhs: ASExpr, rhs: ASExpr) extends ASExpr
case class SFieldName(fn: ASFieldName) extends ASExpr
case class SAgg(op: String, quant: Option[ASSetQuantifier], exprs: Seq[ASExpr]) extends ASExpr
case class SCompOp(op: String, quant: Option[ASComparatorQuantifier], lhs: ASExpr, rhs: ASExpr) extends ASExpr

abstract class ASFieldName
case class SUnqualifiedField(name: String) extends ASFieldName
case class SQualifiedField(qual: String, name: String) extends ASFieldName

abstract class ASComparatorQuantifier
case class SComparatorQuantifierAny() extends ASComparatorQuantifier
case class SComparatorQuantifierAll() extends ASComparatorQuantifier

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
      case UnhandledType(msg) =>
        println("Couldn't parse expression: Unhandled type : " + msg)
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
