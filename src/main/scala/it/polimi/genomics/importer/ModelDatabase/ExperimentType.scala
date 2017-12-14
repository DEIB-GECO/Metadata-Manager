package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.PlatformRetriver

class ExperimentType(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) {

  var technique : String = _

  var feature : String = _

  var target : String = _

  var antibody : String = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => this.feature = insertMethod(this.feature,param)
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case _ => noMatching(dest)

  }

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
