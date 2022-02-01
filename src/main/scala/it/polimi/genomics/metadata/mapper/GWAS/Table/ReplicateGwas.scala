package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.{Replicate}
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId

class ReplicateGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId) with Replicate {
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
  }

  override def checkConsistency(): Boolean = true
}