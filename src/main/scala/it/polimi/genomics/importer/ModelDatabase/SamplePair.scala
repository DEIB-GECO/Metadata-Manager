package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.RemoteDatabase.DbHandler.{database, ontologyTable}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SamplePair extends Table{

  var itemId: Int = _

  var key: String = _

  var value: String = _

  override def getId: Int = ???

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertPair(itemId,key,value)
  }

  override def insert(): Int = {
    dbHandler.insertPair(itemId,key,value)
  }

  override def update(): Int = {
    dbHandler.updatePair(this.itemId,this.key,this.value)
  }

  override def setForeignKeys(table: Table): Unit = ???











}
