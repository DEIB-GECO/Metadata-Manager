package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.DerivedFrom

class DerivedFromTCGA extends DerivedFrom{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
