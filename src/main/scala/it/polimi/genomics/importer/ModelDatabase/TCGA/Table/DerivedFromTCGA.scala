package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.DerivedFrom
import it.polimi.genomics.importer.ModelDatabase.Exception.IllegalOperationException

class DerivedFromTCGA extends DerivedFrom{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in Derived From")
  }
}
