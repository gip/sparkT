package sparkt.backend.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import sparkt.ast.sql._
import sparkt.ast.etl._
import sparkt.ast.database._
import sparkt.backend.mapper._
import sparkt.backend.spark._

object ETL {
  import Context._

  def execute(etl: SETL): Either[String, String] = {
    def doit(arcs: Seq[SArc], nJob: Int): Either[String, String] =
      arcs match {
        case Nil =>
          println("All jobs have been executed")
          nJob
          Right(s"All done after executing ${nJob} steps")
        case _ =>
          val n = arcs.length
          val arcs1 = arcs.filter(arc => !executeArc(arc))
          val n1 = arcs1.length
          if (n==n1) {
            println(s"Could not find a job to execute!")
            Left(s"Could not find a job to execute - an input may be missing")
          } else {
            doit(arcs1, nJob+1)
          }
      }
    doit(etl.arcs, 0)
  }

  def executeArc(arc: SArc): Boolean = {
    if (mapper.isDF(arc.successor.mapping.table, arc.successor.mapping)) {
      println(arc.successor.mapping.table)
      true // The step is done
    } else {
      val ready = arc.predecessors.forall(pred => mapper.isDF(pred.mapping.table, pred.mapping))
      if(ready) {
        println(s"Executing step ${arc.step.name}")
        compiler.compileInsert(arc.step.insert)
      }
      ready
    }
  }

}
