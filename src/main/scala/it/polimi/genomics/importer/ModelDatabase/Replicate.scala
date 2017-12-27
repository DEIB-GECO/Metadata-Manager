package it.polimi.genomics.importer.ModelDatabase

trait Replicate extends Table{

  var bioSampleId : Int = _

  var sourceId : String = _

  var bioReplicateNum : Int = _

  var techReplicateNum : Int = _

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")


  override def insert(): Int = {
    this.dbHandler.insertReplicate(this.bioSampleId,this.sourceId,this.bioReplicateNum,this.techReplicateNum)
  }

  override def update(): Int = {
    this.dbHandler.updateReplicate(this.bioSampleId,this.sourceId,this.bioReplicateNum,this.techReplicateNum)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.bioSampleId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicate(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getReplicateId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

}
