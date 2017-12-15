package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Case

class CaseTCGA extends Case{

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
