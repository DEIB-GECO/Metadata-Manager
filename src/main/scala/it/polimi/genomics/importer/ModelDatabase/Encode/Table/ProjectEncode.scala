package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Project

class ProjectEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with Project {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "PROJECTNAME" => {this.projectName = insertMethod(this.projectName,param); println("Project name " + this.projectName)}
    case "PROGRAMNAME" => this.programName = insertMethod(this.programName,param)
    case _ => noMatching(dest)
  }
}
