package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import exceptions.NoGlobalKeyException
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Exception.UniqueKeyException

abstract class EncodeTable(var encodeTableId: EncodeTableId){

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("[ENCODE] No global key for " + message)
  }

  def uniqueKeyException(message: String): Unit = {
    throw new UniqueKeyException("[ENCODE] Unique key ")
  }


  def nextPosition(globalKey: String, method: String): Unit = {
  }

  def resetPosition(value: Int, size: Int): Int ={
    if(value == size - 1)
      0
    else
      value + 1
  }
}
