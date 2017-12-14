package it.polimi.genomics.importer.ModelDatabase


class Project(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) {

    var projectName: String = _

    var programName: String = _

    override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
      case "PROJECTNAME" => {this.projectName = insertMethod(this.projectName,param); println("Project name " + this.projectName)}
      case "PROGRAMNAME" => this.programName = insertMethod(this.programName,param)
      case _ => noMatching(dest)
    }

  override def insert() = {
    dbHandler.insertProject(this.projectName.toUpperCase(),this.programName)
  }

  override def update() = {
    dbHandler.updateProject(this.projectName.toUpperCase(),this.programName)
  }

  override def setForeignKeys(table: Table): Unit = {
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertProject(this.projectName.toUpperCase())
  }

  override def getId(): Int = {
    dbHandler.getProjectId(this.projectName)
  }

  override def checkConsistency(): Boolean = {
    if(this.projectName != null) true else false
  }
}
