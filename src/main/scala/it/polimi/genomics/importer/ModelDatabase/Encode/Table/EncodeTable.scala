package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import exceptions.NoGlobalKeyException
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId

abstract class EncodeTable(var encodeTableId: EncodeTableId){

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("[ENCODE] No global key for " + message)
  }

}
