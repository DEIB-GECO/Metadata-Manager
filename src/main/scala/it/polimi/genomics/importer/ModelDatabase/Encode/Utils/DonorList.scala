package it.polimi.genomics.importer.ModelDatabase.Encode.Utils

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId

import scala.collection.mutable.ListBuffer

class DonorList(lines: Array[String], encodesTableId: EncodeTableId){

  private val r = "replicates__(\\d)+__library__biosample__accession".r

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
