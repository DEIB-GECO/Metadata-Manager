package it.polimi.genomics.importer.ModelDatabase


class Project extends EncodeTable{

    var projectName: String = "ENCODE"

    var programName: String = _

    override def setParameter(param: String, dest: String): Unit = dest.toUpperCase() match {
      case "PROJECTNAME" => this.projectName = "ENCODE"
      case "PROGRAMNAME" => this.programName = setValue(this.programName,param)
      case _ => noMatching(dest)
    }

  override def insert() = {
    dbHandler.insertProject(this.programName,this.programName)
  }

  override def setForeignKeys(table: Table): Unit = {
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertDonor(this.projectName)
  }

  override def getId() = {
    dbHandler.getProjectId(this.projectName)
  }
}
