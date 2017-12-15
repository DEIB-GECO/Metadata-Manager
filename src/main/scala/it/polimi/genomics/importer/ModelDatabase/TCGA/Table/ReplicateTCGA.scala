package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Replicate

class ReplicateTCGA extends Replicate{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
