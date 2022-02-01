package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.{Case, Donor}
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId

class DonorGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId) with Donor {
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
  }

  override def checkConsistency(): Boolean = true
}
