package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import it.polimi.genomics.metadata.mapper.{Replicate, ReplicateItem}

class ReplicateItemGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId) with ReplicateItem {
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???
}
