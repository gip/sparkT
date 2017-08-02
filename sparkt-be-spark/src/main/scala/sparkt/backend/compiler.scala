package sparkt.backend.compiler

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import sparkt.ast.sql._
import sparkt.ast.database._

case class CompilerUnimplementedConstruct(msg: String) extends Throwable
case class CompilerWrongConstruct(msg: String) extends Throwable

abstract class Mapper(implicit spark: SparkSession) {
  // TODO: check the schema
  def getDF(name: String, info: Option[DDatabaseMapping]): DataFrame
}

class Compiler(spark: SparkSession) {
  import spark.implicits._

  def compileExpr(expr: ASExpr): Column =
    expr match {
      case SFieldName(SUnqualifiedField(name)) => column(name)
      case SLitString(s) => lit(s)
      case SLitInt(i) => lit(i)
      case SLitDouble(d) => lit(d)
      case SBinop("+", lhs, rhs) => compileExpr(lhs) + compileExpr(rhs)
      case SBinop("-", lhs, rhs) => compileExpr(lhs) - compileExpr(rhs)
      case SAgg(op, None, Seq(expr)) =>
        op match {
          case "SUM" => sum(compileExpr(expr))
          case "AVG" => avg(compileExpr(expr))
          case "MAX" => max(compileExpr(expr))
          case "MIN" => min(compileExpr(expr))
          case _ => throw CompilerUnimplementedConstruct("Aggregation function "+op)
        }
      case SAgg(_, Some(_), _) => throw CompilerUnimplementedConstruct("Aggregation quantifier")
      case SAgg(_, _, exprs) => throw CompilerUnimplementedConstruct("Aggregation with expression lists are not supported")
      case _ => throw CompilerUnimplementedConstruct(expr.toString())
    }

  def compileSelect(select: ASSelect)(mapper: Mapper): DataFrame =
    select match {
      case SSelect(selectType, ordering, limit, Some(offset)) => throw CompilerUnimplementedConstruct("Offset not supported")
      case SSelect(selectType, ordering, limit, None) =>
        selectType match {
          case SSelectTable(projs, Some(SFromTable(source, optName)), where, grouping, having) =>
            val df = compilerTableSource(source)(mapper)
            val whereExpr = where map { compileExpr(_) }
            val df1 = whereExpr match {
              case Some(e) => df.filter(e)
              case None => df
            }
            val df2 = optName match {
              case Some(n) => df1.as(n)
              case None => df1
            }
            having match {
              case None => ()
              case Some(_) => throw CompilerUnimplementedConstruct("Having clause not yet supported")
            }
            grouping match {
              case None => df2.select(projs.map(compileProj): _*)
              case Some(SGrouping(groups)) =>
                // Spark keeps the grouped columns except if spark.sql.retainGroupColumns is set to false
                projs.lift(0) match {
                  case Some(p) =>
                    df2.groupBy(groups.map(compileExpr): _*).agg(compileProj(p), projs.drop(1).map(compileProj): _*)
                  case None => throw CompilerWrongConstruct("At least one field must be present in grouping clause")
                }
              case _ => throw CompilerUnimplementedConstruct("Unimplemented grouping clause")
            }
          case SSelectTable(proj, None, where, grouping, having) => throw CompilerUnimplementedConstruct("No origin table")
        }
    }

  def compileProj(p: (ASExpr, Option[String])): Column =
    p._2 match {
      case Some(s) => compileExpr(p._1).as(s)
      case None => compileExpr(p._1)
    }

  def compilerTableSource(source: ASTableSource)(mapper: Mapper): DataFrame =
    source match {
      case STableNamed(info, name) => mapper.getDF(name, info)
      case STableSubSelect(select) => compileSelect(select)(mapper)
    }

}
