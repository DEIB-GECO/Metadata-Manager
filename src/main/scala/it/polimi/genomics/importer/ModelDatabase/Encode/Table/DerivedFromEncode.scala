package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.DerivedFrom
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Exception.IllegalOperationException

class DerivedFromEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with DerivedFrom {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in DerivedFrom")

  }

}
