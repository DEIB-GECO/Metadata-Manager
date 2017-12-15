package it.polimi.genomics.importer.ModelDatabase

trait Project extends Table{

  var projectName: String = _

  var programName: String = _

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
