package it.polimi.genomics.metadata.mapper.Encode.Table

import it.polimi.genomics.metadata.mapper.Case
import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId

class CaseEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with Case {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SOURCESITE" => this.sourceSite = insertMethod(this.sourceSite,param)
    case "EXTERNALREF" => this.externalRef =  insertMethod(this.externalRef,param)
    case _ => noMatching(dest)
  }


  override def insert() = {
    val id = dbHandler.insertCase(this.projectId,this.sourceId,this.sourceSite,this.externalRef)
    this.encodeTableId.caseId_(id)
    id
  }

  override def update() = {
    val id = dbHandler.updateCase(this.projectId,this.sourceId,this.sourceSite,this.externalRef)
    this.encodeTableId.caseId_(id)
    id
  }

  override def updateById() = {
    val id = dbHandler.updateCaseById(this.primaryKey, this.projectId,this.sourceId,this.sourceSite,this.externalRef)
    this.encodeTableId.caseId_(id)
  }
}
