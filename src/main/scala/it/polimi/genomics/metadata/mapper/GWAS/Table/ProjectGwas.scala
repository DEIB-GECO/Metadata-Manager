package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import it.polimi.genomics.metadata.mapper.Project

class ProjectGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId)  with Project{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = dest.toUpperCase() match {
    case "PROJECTNAME" => this.projectName = insertMethod(this.projectName,param)
    case "PROGRAMNAME" => this.programName = insertMethod(this.programName,param)
    case _ => noMatching(dest)
  }
}
