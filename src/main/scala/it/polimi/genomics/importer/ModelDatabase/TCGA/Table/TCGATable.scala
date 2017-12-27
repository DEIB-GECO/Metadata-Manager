package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import exceptions.NoGlobalKeyException

class TCGATable {

  def noMatching(message: String): Unit = {
    throw new NoGlobalKeyException("[TCGA] No global key for" + message)
  }

}
