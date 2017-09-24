package it.polimi.genomics.importer.ModelDatabase


class Container extends EncodeTable{

  var experimentTypeId : Int = _

  var name : String = _

  var assembly: String = _

  var isAnn: Boolean = _

  var annotation: String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE")

  override def setParameter(param: String, dest: String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = setValue(this.name,param)
    case "ASSEMBLY" => this.assembly = setValue(this.assembly,param)
    case "ISANN" => this.isAnn = false
    case "ANNOTATION" => this.annotation = null
    case _ => noMatching(dest)
  }

  override def insert() : Int ={
    dbHandler.insertContainer(experimentTypeId,this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.experimentTypeId = table.getId
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertContainer(this.name)
  }

  override def getId() = {
    dbHandler.getContainerId(this.name)
  }
}
