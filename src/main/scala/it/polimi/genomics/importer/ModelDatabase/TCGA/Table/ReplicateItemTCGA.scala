package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Exception.IllegalOperationException
import it.polimi.genomics.importer.ModelDatabase.ReplicateItem

class ReplicateItemTCGA extends TCGATable with ReplicateItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in Replicate Item")
  }
}
