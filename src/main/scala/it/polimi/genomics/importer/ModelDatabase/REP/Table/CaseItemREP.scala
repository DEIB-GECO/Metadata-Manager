package it.polimi.genomics.importer.ModelDatabase.REP.Table

import it.polimi.genomics.importer.ModelDatabase.CaseItem
import it.polimi.genomics.importer.ModelDatabase.Exception.IllegalOperationException
import it.polimi.genomics.importer.ModelDatabase.REP.REPTableId

class CaseItemREP(repTableId: REPTableId) extends REPTable(repTableId) with CaseItem {

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in CasesItem")

  }
}
