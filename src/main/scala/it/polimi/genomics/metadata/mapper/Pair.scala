package it.polimi.genomics.metadata.mapper

import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler.{database, ontologyTable}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Pair extends Table {

  var itemId: Int = _

  var key: String = _

  var value: String = _

  override def getId: Int = ???

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertPair(itemId, key, value)
  }

  override def insert(): Int = {
    dbHandler.insertPair(itemId, key, value)
  }

  override def update(): Int = ???

  override def setForeignKeys(table: Table): Unit = ???

  def getPairs(itemId: Int): Seq[(String, String)] = dbHandler.getPairs(itemId)


  def insertBatch(itemId: Int, insertPairs: List[(String, String)]): Int = {
    dbHandler.insertPairBatch(itemId, insertPairs)
  }

  def deleteBatch(itemId: Int, deletePairs: List[(String, String)]): Int = {
    dbHandler.deletePairBatch(itemId, deletePairs)
  }


}
