package it.polimi.genomics.metadata.mapper.Encode.Utils

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler.conf

import scala.collection.mutable.ListBuffer

class BioSampleList(lines: Array[String], encodeTableId: EncodeTableId) {

  private val r = conf.getString("import.encode_biosample_accession_pattern").r
  //private val r = "replicates__(\\d)+__library__biosample__accession".r

  private var _list = new ListBuffer[String]()

  for(l <- lines){
    val mi = r.findAllIn(l)
    if(mi.hasNext) {
      val temp = mi.next()
      val replicateNumber = l.split("__", 5)
      _list += replicateNumber(1)
    }
  }

  encodeTableId.bioSampleArray(_list.toArray)

  //def bioSampleQuantity = _list.length

  def BiosampleList: List[String] = _list.toList

}
