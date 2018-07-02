package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.CaseItem
import it.polimi.genomics.importer.ModelDatabase.Exception.IllegalOperationException

class CaseItemTCGA extends CaseItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in Case Item")
  }
}
