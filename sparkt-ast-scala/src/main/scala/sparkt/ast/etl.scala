package sparkt.ast.etl

import sparkt.ast.database._
import sparkt.ast.sql._

case class SDAG(vertices: Seq[SVertex], arcs: Seq[SArc])
case class SVertex(mapping: ADDatabaseMapping)
case class SArc(predecessors: Seq[SVertex], successor: SVertex, step: SProcessingStep)
case class SProcessingStep(name: String, etl: ASInsert)
