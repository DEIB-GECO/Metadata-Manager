package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Encode.Table.{ItemEncode, ReplicateEncode}

import scala.collection.mutable.ListBuffer

trait ReplicateItem extends Table{

  var itemId: Int = _

  var replicateId: Int = _

  var repId: Int = _

  _hasForeignKeys = true

  _foreignKeysTables = List("ITEMS","REPLICATES")

  override def insertRow(): Unit ={
    if(this.checkInsert()) {
      this.insert
    }
  }

  override def insert() : Int ={
    dbHandler.insertReplicateItem(itemId,repId)
  }

  override def update() : Int ={
    -1 //ritorno un valore senza senso in quanto non ci possono essere update per le tabelle di congiunzione, al massimo si inserisce una riga nuova
  }

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[ItemEncode])
      this.itemId = table.primaryKey
    if(table.isInstanceOf[ReplicateEncode])
      this.replicateId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertReplicateItem(itemId,repId)
  }

  override def getId(): Int = {
    -1
  }

}
