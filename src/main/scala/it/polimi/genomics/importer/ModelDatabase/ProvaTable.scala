package it.polimi.genomics.importer.ModelDatabase

import exceptions.NoGlobalKeyException

abstract class ProvaTable(encodeTable: EncodeTableId) {

  var encodeTableId = encodeTable

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("No global key for " + message)
  }

}
