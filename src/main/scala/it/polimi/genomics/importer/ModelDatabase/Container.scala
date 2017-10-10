package it.polimi.genomics.importer.ModelDatabase


class Container extends EncodeTable{

  var experimentTypeId : Int = _

  var name : String = "ENCODE"

  var assembly: String = _

  var isAnn: Boolean = false

  var annotation: String = null

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE")

  override def setParameter(param: String, dest: String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = setValue(this.name,param)
    case "ASSEMBLY" => this.assembly = setValue(this.assembly,param)
    case "ISANN" => this.isAnn = false   //manually curated
    case "ANNOTATION" => this.annotation = null     //manually curated
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

  override def getId(): Int = {
    dbHandler.getContainerId(this.name)
  }
}
