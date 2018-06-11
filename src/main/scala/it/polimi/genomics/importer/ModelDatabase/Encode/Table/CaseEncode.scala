package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Case
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId

class CaseEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with Case {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SOURCESITE" => this.sourceSite = insertMethod(this.sourceSite,param)
    case "EXTERNALREF" => this.externalRef =  insertMethod(this.externalRef,param)
    case _ => noMatching(dest)
  }


  override def insert() = {
    val id = dbHandler.insertCase(this.containerId,this.sourceId,this.sourceSite,this.externalRef)
    this.encodeTableId.caseId_(id)
    id
  }

  override def update() = {
    val id = dbHandler.updateCase(this.containerId,this.sourceId,this.sourceSite,this.externalRef)
    this.encodeTableId.caseId_(id)
    id
  }

  override def updateById() = {
    val id = dbHandler.updateCaseById(this.primaryKey, this.containerId,this.sourceId,this.sourceSite,this.externalRef)
    this.encodeTableId.caseId_(id)
  }
}
