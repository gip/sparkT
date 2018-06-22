package sparkt.backend.schema

import java.net.URI
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import sparkt.ast.database._

object Schema {

  def toSparkSchema(schema: Seq[(String, ATType, Boolean)]) : StructType = {
    def doit(columns: Seq[(String, ATType, Boolean)]): List[StructField] =
      columns match {
      case Nil => Nil
      case (name, atype, nullable)::cols =>
        var rtype = atype match {
          case TInt() => LongType
          case TDouble() => DoubleType
          case TString() => StringType
          case TText() => StringType
          case TBool() => BooleanType
        }
        StructField(name, rtype, nullable) :: doit(cols)
      }

    StructType(doit(schema))
    }

}
