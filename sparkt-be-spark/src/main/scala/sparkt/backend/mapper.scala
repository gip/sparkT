package sparkt.backend.mapper

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
import sparkt.backend.schema._

abstract class Mapper {

  // TODO: check the schema
  def getDF(name: String, info: DDatabaseMapping): Try[DataFrame]
  def setDF(data: DataFrame, name: String, info: DDatabaseMapping): Try[Unit]
  def createDF(name: String, info: DDatabaseMapping): DataFrame
  def getSchema(name: String, info: DDatabaseMapping): StructType =
    Schema.toSparkSchema(info.schema._2.map(a => a._1 -> a._2).toMap.apply(name))
  def isDF(name: String, info: DDatabaseMapping): Boolean
}

class SimpleMapper(implicit spark: SparkSession) extends Mapper {

  def isDF(name: String, info: DDatabaseMapping) : Boolean =
    readString(s"${info.url}/${name}/_SUCCESS").isSuccess

  def getDF(name: String, info: DDatabaseMapping) : Try[DataFrame] = {
    val url = info.url + name
    val schema = Schema.toSparkSchema(info.schema._2.map(a => a._1 -> a._2).toMap.apply(name))
    readWithSchema(url)
  }

  def setDF(df: DataFrame, name: String, info: DDatabaseMapping) : Try[Unit] = {
    val url = info.url + name
    val schema = Schema.toSparkSchema(info.schema._2.map(a => a._1 -> a._2).toMap.apply(name))
    writeWithSchema(df, url)
  }

  def createDF(name: String, info: DDatabaseMapping) : DataFrame = {
    val schema = Schema.toSparkSchema(info.schema._2.map(a => a._1 -> a._2).toMap.apply(name))
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  private[this] val schemaFile = "_SCHEMA.json"

  def readWithSchema(dir: String)(implicit spark: SparkSession): Try[DataFrame] =
    for {
      jsonSchema <- readString(s"${dir}/${schemaFile}")
      val schema = DataType.fromJson(jsonSchema).asInstanceOf[StructType]
      df <- readCSV(dir, schema, header = false)
    } yield df

  def writeWithSchema(data: DataFrame, dir: String)(implicit spark: SparkSession): Try[Unit] =
    for {
      _ <- writeCSV(data, dir, header = false)
      _ <- writeString(s"${dir}/${schemaFile}", data.schema.prettyJson)
    } yield ()

  def writeCSV(data: DataFrame, dir: String, header: Boolean): Try[Unit] =
    Try {
      data
        .write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", header)
        .save(dir)
    }

  def readCSV(dir: String, schema: StructType, header: Boolean): Try[DataFrame] =
    Try {
      spark.read.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", header)
        .schema(schema)
        .load(dir)
    }

  def readString(file: String)(implicit spark: SparkSession): Try[String] =
    fsys(file).flatMap(fs => Try {
      val is = fs.open(new Path(file))
      try {
        Source.fromInputStream(is).mkString
      } finally {
        is.close()
      }
    })

  def writeString(file: String, value: String)(implicit spark: SparkSession): Try[Unit] =
    fsys(file).flatMap(fs => Try {
      val os = fs.create(new Path(file))
      try {
        os.write(value.getBytes("utf-8"))
      } finally {
        os.close()
      }
    })

  def fsys(x: String)(implicit spark: SparkSession): Try[FileSystem] =
    Try { FileSystem.get(new URI(x), spark.sparkContext.hadoopConfiguration) }

  def ls(dir: String)(implicit spark: SparkSession): Try[Array[FileStatus]] =
    fsys(dir).flatMap(fs => Try { fs.listStatus(new Path(dir)) })
}
