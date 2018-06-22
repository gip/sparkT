package sparkt.ast.etl

import sparkt.ast.database._
import sparkt.ast.sql._

case class SETL(identifier: String, vertices: Seq[SVertex], arcs: Seq[SArc])
case class SVertex(mapping: DDatabaseMapping)
case class SArc(predecessors: Seq[SVertex], successor: SVertex, step: SStep)
case class SStep(name: String, insert: ASInsert)
