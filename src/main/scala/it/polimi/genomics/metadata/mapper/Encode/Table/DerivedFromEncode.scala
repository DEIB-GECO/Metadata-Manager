package it.polimi.genomics.metadata.mapper.Encode.Table

import it.polimi.genomics.metadata.mapper.DerivedFrom
import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.Exception.IllegalOperationException

class DerivedFromEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with DerivedFrom {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    throw new IllegalOperationException("Set parameter non deve essere richiamato in DerivedFrom")

  }

}
