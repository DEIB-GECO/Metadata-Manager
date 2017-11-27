package it.polimi.genomics.importer.ModelDatabase

import scala.collection.mutable.ListBuffer


class ReplicateItem extends EncodeTable{

  var itemId: Int = _

  var replicateId: ListBuffer[Int] = new ListBuffer[Int]

  _hasForeignKeys = true

  _foreignKeysTables = List("ITEMS","REPLICATES")

  var actualPosition: Int = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = ???

  override def insertRow(): Unit ={
    this.replicateId.map(replicate=>{
      this.actualPosition = replicateId.indexOf(replicate)
      if(this.checkInsert()) {
        this.insert()
      }})
  }

  override def insert() : Int ={
    dbHandler.insertReplicateItem(itemId,replicateId(this.actualPosition))
  }

  override def update() : Int ={
   -1 //ritorno un valore senza senso in quanto non ci possono essere update per le tabelle di congiunzione, al massimo si inserisce una riga nuova
  }

 /* def insertReplicateItem(replicateId: Int) : Int ={
    dbHandler.insertReplicateItem(itemId,replicateId)
  }*/

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[Item])
      this.itemId = table.primaryKey
    if(table.isInstanceOf[Replicate])
      this.replicateId = table.primaryKeys
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicateItem(this.itemId,this.replicateId(this.actualPosition))
  }

  /*def checkInsertReplicateItem(replicateId: Int): Boolean ={
    dbHandler.checkInsertReplicateItem(this.itemId,replicateId)
  }*/

  override def getId(): Int = {
    -1
  }

}
