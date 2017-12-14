package it.polimi.genomics.importer.ModelDatabase


import exceptions.NoGlobalKeyException

abstract class EncodeTable(var encodeTableId: EncodeTableId) extends Table{

  override def insertRow(): Unit ={
    if(this.checkInsert()) {
      val id = this.insert
      this.primaryKey_(id)
    }
    else {
      //val id = this.getId
      val id = this.update
      this.primaryKey_(id)
    }
  }

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("No global key for " + message)
  }

  override def checkConsistency(): Boolean = {
    true
  }
}
