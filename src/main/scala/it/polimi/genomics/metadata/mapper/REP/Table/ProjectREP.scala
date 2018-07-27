package it.polimi.genomics.metadata.mapper.REP.Table

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.Project
import it.polimi.genomics.metadata.mapper.REP.REPTableId

class ProjectREP(repTableId: REPTableId) extends REPTable(repTableId) with Project {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "PROJECTNAME" => this.projectName = insertMethod(this.projectName,param)
    case "PROGRAMNAME" => this.programName = insertMethod(this.programName,param)
    case _ => noMatching(dest)
  }
}
