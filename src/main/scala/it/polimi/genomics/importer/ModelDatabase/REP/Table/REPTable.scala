package it.polimi.genomics.importer.ModelDatabase.REP.Table

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Exception.UniqueKeyException
import it.polimi.genomics.importer.ModelDatabase.REP.REPTableId
import org.apache.log4j.Logger

abstract class REPTable(var repTableId: REPTableId){

  private val loggerTable: Logger = Logger.getLogger(this.getClass)
  private val conf = ConfigFactory.load()

  def noMatching(message: String): Unit = {
    this.loggerTable.warn("No Global key for " + message)
  }

  def uniqueKeyException(message: String): Unit = {
    throw new UniqueKeyException("[REP] Unique key ")
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
