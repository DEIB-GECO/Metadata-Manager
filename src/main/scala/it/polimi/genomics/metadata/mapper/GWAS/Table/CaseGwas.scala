package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.Case
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId

class CaseGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId) with Case {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SOURCESITE" => this.sourceSite = insertMethod(this.sourceSite,param)
    case "EXTERNALREF" => this.externalRef =  insertMethod(this.externalRef,param)
    case _ => noMatching(dest)
  }
}
