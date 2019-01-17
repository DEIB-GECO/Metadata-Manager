package it.polimi.genomics.metadata.step

import it.polimi.genomics.metadata.step.xml.Source
import org.slf4j.LoggerFactory


trait Step {
  def execute(source: Source, parallelExecution: Boolean): Unit

}

object Step {

  import it.polimi.genomics.metadata.step.utils.ExecutionLevel._

  val logger = LoggerFactory.getLogger(this.getClass)

  def getLevelExecutable(key: ExecutionLevel): Step = key match {
    case Transform => TransformerStep
    case Clean => CleanerStep
    //    case Map => Mapper
    //    case Enrich => enricherEnabled
    case Flatten => FlattenerStep
    case _ => new Step {
      override def execute(source: Source, parallelExecution: Boolean): Unit = {
        logger.error("Unknown level please check and correct the implementation")
        throw new Exception("Unknown level please check and correct the implementation")
      }
    }
  }
}

