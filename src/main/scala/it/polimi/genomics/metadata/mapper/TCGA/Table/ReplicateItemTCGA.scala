package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException
import it.polimi.genomics.metadata.mapper.ReplicateItem

class ReplicateItemTCGA extends TCGATable with ReplicateItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in Replicate Item")
  }
}
