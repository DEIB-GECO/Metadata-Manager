package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.CaseItem
import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException

class CaseItemTCGA extends CaseItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in Case Item")
  }
}
