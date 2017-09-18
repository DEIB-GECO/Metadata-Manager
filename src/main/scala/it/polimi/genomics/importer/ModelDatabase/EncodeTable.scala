package it.polimi.genomics.importer.ModelDatabase


import exceptions.NoGlobalKeyException

abstract class EncodeTable extends Table{

  def setValue(actualParam: String, newParam: String): String = {
    if(actualParam == null) {
      return newParam
    }
    else
      return actualParam.concat(" " + newParam)
  }

  def checkInsert(): Boolean = true

  def insert(): Int = 1

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("No global key for " + message)
  }
}
