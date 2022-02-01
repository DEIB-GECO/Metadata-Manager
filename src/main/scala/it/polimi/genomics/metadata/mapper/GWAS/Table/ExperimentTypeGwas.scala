package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.ExperimentType
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId

class ExperimentTypeGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId)  with ExperimentType {
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case _ => noMatching(dest)

  }
}
