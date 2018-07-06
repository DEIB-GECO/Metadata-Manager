package it.polimi.genomics.importer.ModelDatabase.REP.Utils

import it.polimi.genomics.importer.ModelDatabase.REP.REPTableId
import it.polimi.genomics.importer.RemoteDatabase.DbHandler.conf

import scala.collection.mutable.ListBuffer

class BioSampleList(lines: Array[String], repTableId: REPTableId) {

  private val r = conf.getString("import.rep_biosample_accession_pattern").r

  private var _list = new ListBuffer[String]()

  for(l <- lines){
    val mi = r.findAllIn(l)
    if(mi.hasNext) {
      val temp = mi.next()
      val replicateNumber = l.split("\t")(0).split("__", 3)
      _list += replicateNumber(2)
    }
  }
  if(_list.isEmpty)
    _list += "1"


  repTableId.bioSampleArray(_list.toArray)

  def BiosampleList: List[String] = _list.toList

}
