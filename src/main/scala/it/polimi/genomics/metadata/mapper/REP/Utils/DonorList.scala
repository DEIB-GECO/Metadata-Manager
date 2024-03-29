package it.polimi.genomics.metadata.mapper.REP.Utils

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler.conf

import scala.collection.mutable.ListBuffer

class DonorList(lines: Array[String], encodesTableId: EncodeTableId){

  private val r = conf.getString("import.biosample_accession_pattern").r
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

  encodesTableId.bioSampleArray(_list.toArray)

  def bioSampleQuantity = _list.length

  def DonorList: List[String] = _list.toList

}
