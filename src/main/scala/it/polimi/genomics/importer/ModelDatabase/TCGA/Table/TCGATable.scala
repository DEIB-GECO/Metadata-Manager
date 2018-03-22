package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import org.apache.log4j.Logger


class TCGATable {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def noMatching(message: String): Unit = {
    this.logger.warn("No Global key for " + message)
  }

}
