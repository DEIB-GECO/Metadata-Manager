package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Project

class ProjectTCGA extends Project{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
