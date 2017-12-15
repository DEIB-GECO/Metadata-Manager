package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.CaseItem

class CaseItemTCGA extends CaseItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
