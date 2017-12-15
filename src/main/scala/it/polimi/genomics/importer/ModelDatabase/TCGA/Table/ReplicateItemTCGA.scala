package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.ReplicateItem

class ReplicateItemTCGA extends ReplicateItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
