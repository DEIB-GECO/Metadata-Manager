package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Container

class ContainerTCGA extends Container{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
