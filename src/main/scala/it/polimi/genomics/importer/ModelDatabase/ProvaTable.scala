package it.polimi.genomics.importer.ModelDatabase

import exceptions.NoGlobalKeyException

abstract class ProvaTable {

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("No global key for " + message)
  }

}
