package sparkt.backend.compiler

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import sparkt.ast.sql._
import sparkt.ast.database._
import sparkt.backend.mapper._

case class CompilerUnimplementedConstruct(msg: String) extends Throwable
case class CompilerWrongConstruct(msg: String) extends Throwable
case class CompilerSchemaNotMatchingError(msg: String) extends Throwable

class Compiler(spark: SparkSession)(mapper: Mapper) {
  import spark.implicits._
  val sqlContext = spark.sqlContext

  def compileInsert(insert: ASInsert): Unit =
    insert match {
      case SInsert(info, name, fields, values) =>
        values match {
          case SInsertValues(values) =>
            val rows = values.map(row => Row.fromSeq(row.map(compileLitExpr)))
            println(rows)
            val rdd = sqlContext.sparkContext.parallelize(rows)
            val df = sqlContext.createDataFrame(rdd, mapper.getSchema(name, info))
            mapper.setDF(df, name, info)
          case SInsertSelect(select) =>
            val selectDF = compileSelect(select)
            if(fields.isEmpty) {
              mapper.setDF(selectDF, name, info)
            } else {
              val schema = mapper.getSchema(name, info)
              // TODO: check that fields and schema match
              val df = selectDF.toDF(fields: _*)
              mapper.setDF(df, name, info)
            }
        }
    }

  def compileLitExpr(expr: ASExpr): Any =
    expr match {
      case SLitString(s) => s
      case SLitInt(i) => i.asInstanceOf[Long]
      case SLitDouble(d) => d
    }


  def compileExpr(expr: ASExpr): Column =
    expr match {
      case SFieldName(SUnqualifiedField(name)) => column(name)
      //case SFieldName(SQualifiedField(scope,name)) => column(scope+"."+name)
      case SFieldName(SQualifiedField(scope,name)) => column(name)
      case SLitString(s) => lit(s)
      case SLitInt(i) => lit(i)
      case SLitDouble(d) => lit(d)

      case SBinop("+", lhs, rhs) => compileExpr(lhs) + compileExpr(rhs)
      case SBinop("-", lhs, rhs) => compileExpr(lhs) - compileExpr(rhs)
      case SBinop("*", lhs, rhs) => compileExpr(lhs) * compileExpr(rhs)
      case SBinop("/", lhs, rhs) => compileExpr(lhs) / compileExpr(rhs)
      case SBinop("LIKE", lhs, SLitString(str)) => compileExpr(lhs).like(str)

      case SCompOp("==", quant, lhs, rhs) => compileExpr(lhs) === compileExpr(rhs)
      case SCompOp("<", quant, lhs, rhs) => compileExpr(lhs) < compileExpr(rhs)

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

      case expr =>
        println(expr)
        throw CompilerUnimplementedConstruct(expr.toString())
    }

  def compileSelect(select: ASSelect): DataFrame =
    select match {
      case SSelect(selectType, ordering, limit, Some(offset)) => throw CompilerUnimplementedConstruct("Offset not supported")
      case SSelect(selectType, ordering, limit, None) =>
        selectType match {
          case SSelectTable(projs, Some(from), where, grouping, having) =>
            val df = compileFrom(from)
            val whereExpr = where.map(compileExpr)
            val df1 = whereExpr match {
              case Some(e) => df.filter(e)
              case None => df
            }
            val df2 = df1
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
            df2.select(projs.map(compileProj): _*)
          case SSelectTable(proj, None, where, grouping, having) => throw CompilerUnimplementedConstruct("No origin table")
          case select =>
            println(select)
            throw CompilerUnimplementedConstruct("Unimplemented")
        }
    }

  def compileFrom(from: ASFrom): DataFrame =
    from match {
      case SFromTable(source, optName) =>
        val df = compilerTableSource(source)(mapper)
        optName match {
          case Some(n) => df.as(n)
          case None => df
        }
      case SInnerJoin(lhs, rhs, None) =>
        compileFrom(lhs).join(compileFrom(rhs))
    }

  def compileProj(p: (ASExpr, Option[String])): Column =
    p._2 match {
      case Some(s) => compileExpr(p._1).as(s)
      case None => compileExpr(p._1)
    }

  def compilerTableSource(source: ASTableSource)(mapper: Mapper): DataFrame =
    source match {
      case STableNamed(info, name) => mapper.getDF(name, info).get
      case STableSubSelect(select) => compileSelect(select)
    }

}
