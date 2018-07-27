package it.polimi.genomics.metadata.mapper.REP.Table

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException
import it.polimi.genomics.metadata.mapper.REP.REPTableId
import it.polimi.genomics.metadata.mapper.{ReplicateItem, Table}

import scala.collection.mutable.ListBuffer


class ReplicateItemREP(repTableId: REPTableId) extends REPTable(repTableId) with ReplicateItem {

  var replicateIdList: ListBuffer[Int] = new ListBuffer[Int]

  var actualPosition: Int = _

  var repId: Int = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in ReplicateItem")

  }


  override def insertRow(): Int = {
    this.replicateIdList.map(replicate=>{
      this.actualPosition = replicateIdList.indexOf(replicate)
      if(this.checkInsert()) {
        this.insert()
      }})
    -100
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

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[ItemREP])
      this.itemId = table.primaryKey
    if(table.isInstanceOf[ReplicateREP])
      this.replicateIdList = table.primaryKeys
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertReplicateItem(this.itemId,this.replicateIdList(this.actualPosition))
  }

  override def getId(): Int = {
    -1
  }

}
