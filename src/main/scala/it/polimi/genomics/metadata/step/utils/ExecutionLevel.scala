package it.polimi.genomics.metadata.step.utils

object ExecutionLevel extends Enumeration {
  type ExecutionLevel = Value
  val Download, Transform, Load, Clean, Map, Enrich, Flatten = Value
}