package it.polimi.genomics.importer.ModelDatabase


class Donor extends EncodeTable{

  var sourceId: String = _

  var species : String = _

  var age : Int = _

  var gender : String= _

  var ethnicity : String = _


  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit ={
    dest.toUpperCase() match {
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId, param)
      case "SPECIES" => this.species = insertMethod(this.species, param)
      case "AGE" => age = param.toInt
      case "GENDER" => this.gender = insertMethod(this.gender, param)
      case "ETHNICITY" => this.ethnicity = insertMethod(this.ethnicity, param)
      case _ => noMatching(dest)
    }
  }

  override def insert(): Int ={
    dbHandler.insertDonor(this.sourceId,this.species,this.age,this.gender,this.ethnicity)
  }

  override def update(): Int ={
    dbHandler.updateDonor(this.sourceId,this.species,this.age,this.gender,this.ethnicity)
  }

  override def setForeignKeys(table: Table): Unit = {
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertDonor(this.sourceId)
  }

  def getId(): Int = {
    dbHandler.getDonorId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }
}
