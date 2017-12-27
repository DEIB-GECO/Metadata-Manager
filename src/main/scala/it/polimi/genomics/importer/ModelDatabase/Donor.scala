package it.polimi.genomics.importer.ModelDatabase

trait Donor extends Table{

  var sourceId: String = _

  var species : String = _

  var age : Int = _

  var gender : String= _

  var ethnicity : String = _


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

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

  override def getId(): Int = {
    dbHandler.getDonorId(this.sourceId)
  }

}
