package it.polimi.genomics.importer.ModelDatabase.Utils

import it.polimi.genomics.importer.ModelDatabase.EncodesTableId

import scala.collection.mutable.ListBuffer

class BioSampleList(lines: Array[String]) {

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

  EncodesTableId.bioSampleArray(_list.toArray)

  def bioSampleQuantity = _list.length

  def BiosampleList: List[String] = _list.toList

}
