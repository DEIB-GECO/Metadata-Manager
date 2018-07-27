package it.polimi.genomics.importer.GMQLImporter

import org.slf4j.LoggerFactory

object ExecutionLevel extends Enumeration {
  type ExecutionLevel = Value
  val Download, Transform, Load, Clean, Map, Enrich, Flatten = Value
}


trait Executable {
  def execute(source: GMQLSource, parallelExecution: Boolean): Unit



}

object Executable{
  import ExecutionLevel._

  val logger = LoggerFactory.getLogger(this.getClass)

  def getLevelExecutable(key:ExecutionLevel):Executable = key match {
    case Transform => Transformer
    case Clean => Cleaner
//    case Map => Mapper
//    case Enrich => enricherEnabled
//    case Flatten => flattenerEnabled
    case _ => new Executable {
      override def execute(source: GMQLSource, parallelExecution: Boolean): Unit = {
        logger.error("Unknown level please check and correct the implemtation")

      }
    }
  }
}