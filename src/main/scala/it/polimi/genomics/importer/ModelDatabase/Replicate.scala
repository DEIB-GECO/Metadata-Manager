package it.polimi.genomics.importer.ModelDatabase


class Replicate extends EncodeTable{

  var bioSampleId : Int = _

  var sourceId :String = _

  var bioReplicateNum : Int = _

  var techReplicateNum : Int = _

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")

  override def setParameter(param: String, dest: String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => this.sourceId = setValue(this.sourceId,param)
      case "BIOREPLICATENUM" => this.bioReplicateNum = param.toInt
      case "TECHREPLICATENUM" => this.techReplicateNum = param.toInt
      case _ => noMatching(dest)
    }
  }

  override def insert() = {
    dbHandler.insertReplicate(this.bioSampleId,this.sourceId,this.bioReplicateNum,this.techReplicateNum)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.bioSampleId = table.getId
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertDonor(this.sourceId)
  }

  override def getId() = {
    dbHandler.getReplicateId(this.sourceId)
  }
}
