package it.polimi.genomics.importer.ModelDatabase

trait ExperimentType extends Table{

  var technique : String = _

  var feature : String = _

  var target : String = _

  var antibody : String = _

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertExperimentType(this.technique)
  }

  override def insert(): Int = {
    dbHandler.insertExperimentType(this.technique,this.feature,this.target,this.antibody)
  }

  override def update(): Int = {
    dbHandler.updateExperimentType(this.technique,this.feature,this.target,this.antibody)
  }

  override def checkConsistency(): Boolean = {
    if(this.technique != null) true else false
  }

  override def setForeignKeys(table: Table): Unit = {

  }

  override def getId(): Int = {
    dbHandler.getExperimentTypeId(this.technique)
  }

}
