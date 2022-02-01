package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.CaseItem
import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId

class CaseItemGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId)  with CaseItem{
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in Case Item")
  }
}
