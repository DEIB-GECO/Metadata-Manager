package it.polimi.genomics.importer.ModelDatabase

class Case extends EncodeTable{

    var projectId : Int = _

    var sourceId : String = _

    var sourceSite : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("PROJECTS")

  override def setParameter(param: String, dest: String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = setValue(this.sourceId,param)
    case "SOURCESITE" => this.sourceSite = setValue(this.sourceSite,param)
    case _ => noMatching(dest)
  }


  override def insert() = {
    dbHandler.insertCase(this.projectId,this.sourceId,this.sourceSite)

  }

  override def setForeignKeys(table: Table): Unit = {
    this.projectId = table.getId
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertCaseId(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getCaseId(this.sourceId)
  }

}
