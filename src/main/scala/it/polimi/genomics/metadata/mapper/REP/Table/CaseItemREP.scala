package it.polimi.genomics.metadata.mapper.REP.Table

import it.polimi.genomics.metadata.mapper.CaseItem
import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException
import it.polimi.genomics.metadata.mapper.REP.REPTableId

class CaseItemREP(repTableId: REPTableId) extends REPTable(repTableId) with CaseItem {

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in CasesItem")

  }
}
