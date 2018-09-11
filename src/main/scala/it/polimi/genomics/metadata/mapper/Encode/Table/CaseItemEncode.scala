package it.polimi.genomics.metadata.mapper.Encode.Table

import it.polimi.genomics.metadata.mapper.CaseItem
import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException

class CaseItemEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with CaseItem {

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in CasesItem")

  }
}
