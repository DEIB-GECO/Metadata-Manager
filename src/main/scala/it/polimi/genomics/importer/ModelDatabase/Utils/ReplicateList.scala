package it.polimi.genomics.importer.ModelDatabase.Utils

import scala.collection.mutable.ListBuffer

class ReplicateList(lines: Array[String]) {

  private val r = "replicates__(\\d)+__uuid".r

  private var list = new ListBuffer[String]()

  for(l <- lines){
    val mi = r.findAllIn(l)
    if(mi.hasNext) {
      val temp = mi.next()
      val replicateNumber = l.split("__", 3)
      list += replicateNumber(1)
    }
  }

  private val _replicateList = list.toList

  def replicateList = _replicateList

}
