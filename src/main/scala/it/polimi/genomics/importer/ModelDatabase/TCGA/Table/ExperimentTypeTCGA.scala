package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.ExperimentType

class ExperimentTypeTCGA extends ExperimentType{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
