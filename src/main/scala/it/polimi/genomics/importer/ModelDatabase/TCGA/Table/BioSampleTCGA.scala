package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.BioSample

class BioSampleTCGA extends BioSample {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???

}
