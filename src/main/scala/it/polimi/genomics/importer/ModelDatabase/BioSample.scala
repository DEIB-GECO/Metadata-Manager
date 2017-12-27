package it.polimi.genomics.importer.ModelDatabase

trait BioSample extends Table{

  var donorId: Int = _

  var sourceId: String = _

  var types: String = _

  var tIssue: String = _

  var cellLine: String = _

  var isHealty: Boolean = _

  var disease: String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("DONORS")

  override def insert(): Int ={
    dbHandler.insertBioSample(donorId,this.sourceId,this.types,this.tIssue,this.cellLine,this.isHealty,this.disease)
  }

  override def update(): Int = {
    dbHandler.updateBioSample(donorId,this.sourceId,this.types,this.tIssue,this.cellLine,this.isHealty,this.disease)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.donorId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertBioSample(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getBioSampleId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }


}
