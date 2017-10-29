package it.polimi.genomics.importer.ModelDatabase


import exceptions.NoGlobalKeyException
import it.polimi.genomics.importer.RemoteDatabase.DbHandler

abstract class EncodeTable extends Table{

  protected val dbHandler = DbHandler


  override def insertRow(): Unit ={
    if(this.checkInsert()) {
      val id = this.insert
      this.primaryKey_(id)
    }
    else {
      val id = this.getId
      this.primaryKey_(id)
    }
  }


  def setValue(actualParam: String, newParam: String): String = {
    if(actualParam == null) {
      return newParam
    }
    else
      return actualParam.concat(" " + newParam)
  }

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("No global key for " + message)
  }
}
