package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Item

class ItemTCGA extends Item{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
