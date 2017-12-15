package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.{ReplicateItem, Table}

import scala.collection.mutable.ListBuffer


class ReplicateItemEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with ReplicateItem {

  var replicateIdList: ListBuffer[Int] = new ListBuffer[Int]

  var actualPosition: Int = _


  override def insertRow(): Unit ={
    this.replicateIdList.map(replicate=>{
      this.actualPosition = replicateIdList.indexOf(replicate)
      if(this.checkInsert()) {
        this.insert()
      }})
  }

  def insRow(): Unit = {
    if(this.specialCheckInsert())
      this.specialInsert()
  }

  def specialInsert(): Int = {
    dbHandler.insertReplicateItem(itemId,repId)
  }

  def specialCheckInsert(): Boolean ={
    dbHandler.checkInsertReplicateItem(itemId,repId)
  }

  override def insert() : Int ={
    dbHandler.insertReplicateItem(itemId,replicateIdList(this.actualPosition))
  }

  override def update() : Int ={
    -1 //ritorno un valore senza senso in quanto non ci possono essere update per le tabelle di congiunzione, al massimo si inserisce una riga nuova
  }

  /* def insertReplicateItem(replicateId: Int) : Int ={
     dbHandler.insertReplicateItem(itemId,replicateId)
   }*/

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[ItemEncode])
      this.itemId = table.primaryKey
    if(table.isInstanceOf[ReplicateEncode])
      this.replicateIdList = table.primaryKeys
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertReplicateItem(this.itemId,this.replicateIdList(this.actualPosition))
  }

  /*def checkInsertReplicateItem(replicateId: Int): Boolean ={
    dbHandler.checkInsertReplicateItem(this.itemId,replicateId)
  }*/

  override def getId(): Int = {
    -1
  }

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = ???

}
