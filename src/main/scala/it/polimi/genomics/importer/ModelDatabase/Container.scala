package it.polimi.genomics.importer.ModelDatabase

trait Container extends Table{

  var experimentTypeId : Int = _

  var name : String = "ENCODE"

  var assembly: String = _

  var isAnn: Boolean = _

  var annotation: String = null

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE")

  override def insert() : Int ={
    dbHandler.insertContainer(experimentTypeId,this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def update() : Int ={
    dbHandler.updateContainer(experimentTypeId,this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.experimentTypeId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertContainer(this.name)
  }

  override def getId(): Int = {
    dbHandler.getContainerId(this.name)
  }

  override def checkConsistency(): Boolean = {
    if(this.name != null) true else false
  }
}
