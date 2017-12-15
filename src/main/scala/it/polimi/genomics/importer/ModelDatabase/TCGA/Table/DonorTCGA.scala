package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Donor

class DonorTCGA extends Donor{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
