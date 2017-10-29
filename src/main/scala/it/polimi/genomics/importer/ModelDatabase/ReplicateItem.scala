package it.polimi.genomics.importer.ModelDatabase

import scala.collection.mutable.ListBuffer


class ReplicateItem extends EncodeTable{

  var itemId: Int = _

  var replicateId: ListBuffer[Int] = new ListBuffer[Int]

  _hasForeignKeys = true

  _foreignKeysTables = List("ITEMS","REPLICATES")

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = ???

  override def insertRow(): Unit ={
    this.replicateId.map(replicate=>{
      if(this.checkInsertReplicateItem(replicate)) {
        this.insertReplicateItem(replicate)
      }})
  }

  override def insert() : Int ={
    dbHandler.insertReplicateItem(itemId,replicateId.head)
  }

  def insertReplicateItem(replicateId: Int) : Int ={
    dbHandler.insertReplicateItem(itemId,replicateId)
  }

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[Item])
      this.itemId = table.primaryKey
    if(table.isInstanceOf[Replicate])
      this.replicateId = table.primaryKeys
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicateItem(this.itemId,this.replicateId.head)
  }

  def checkInsertReplicateItem(replicateId: Int): Boolean ={
    dbHandler.checkInsertReplicateItem(this.itemId,replicateId)
  }

  override def getId(): Int = {
    -1
  }

}
