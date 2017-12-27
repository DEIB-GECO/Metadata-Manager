package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.DerivedFrom
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId

class DerivedFromEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with DerivedFrom {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???

}
