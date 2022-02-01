package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.Exception.UniqueKeyException
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import org.apache.log4j.Logger

abstract class GwasTable(var gwasTableId: GwasTableId) {

  private val loggerTable: Logger = Logger.getLogger(this.getClass)

  def noMatching(message: String): Unit = {
    this.loggerTable.warn("No Global key for " + message)
  }

  def uniqueKeyException(message: String): Unit = {
    throw new UniqueKeyException("[ENCODE] Unique key ")
  }


  def nextPosition(globalKey: String, method: String): Unit = {
  }

  def resetPosition(value: Int, size: Int): Int ={
    if(value == size - 1)
      0
    else
      value + 1
  }
}
