package sparkt.backend.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import sparkt.backend.compiler._
import sparkt.backend.mapper._

object Context {
  implicit val spark = SparkSession
     .builder()
     .appName("SparkT")
     .master("local")
     .getOrCreate()
  val mapper = new SimpleMapper
  val compiler = new Compiler(spark)(mapper)
}
