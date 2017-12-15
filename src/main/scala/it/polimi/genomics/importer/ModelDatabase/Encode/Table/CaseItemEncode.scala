package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.CaseItem
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId

class CaseItemEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with CaseItem {

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = ???
}
