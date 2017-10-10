package it.polimi.genomics.importer.ModelDatabase

class ExperimentType extends EncodeTable{

  var technique : String = _

  var feature : String = _

  var platform : String = _

  var target : String = _

  var antibody : String = _

  override def setParameter(param: String, dest: String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = setValue(this.technique,param)
    case "FEATURE" => this.feature = setValue(this.feature,param)
    case "PLATFORM" => this.platform = setValue(this.platform,param)
    case "TARGET" => this.target = setValue(this.target,param)
    case "ANTIBODY" => this.antibody = setValue(this.antibody,param)
    case _ => noMatching(dest)

  }

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertExperimentType(this.technique,this.platform)
  }

  override def insert(): Int = {
    dbHandler.insertExperimentType(this.technique,this.feature,this.platform,this.target,this.antibody)
  }

  override def setForeignKeys(table: Table): Unit = {

  }

  override def getId(): Int = {
    dbHandler.getExperimentTypeId(this.technique,this.platform)
  }
}
