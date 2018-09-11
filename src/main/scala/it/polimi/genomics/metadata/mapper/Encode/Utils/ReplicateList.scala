package it.polimi.genomics.metadata.mapper.Encode.Utils

import scala.collection.mutable.ListBuffer

class ReplicateList(lines: Array[String], bioSampleList: BioSampleList) {

  //private val r = "replicates__(\\d)+__uuid".r

  private var _uuidList = new ListBuffer[String]()
  private var _technicalReplicateNumberList = new ListBuffer[String]()
  private var _biologicalReplicateNumber = new ListBuffer[String]()


  bioSampleList.BiosampleList.foreach(bioReplicateNumber => {
    val r = ("replicates__" + bioReplicateNumber +"__uuid").r
    for(l <- lines){
      val mi = r.findAllIn(l)
      if(mi.hasNext) {
        mi.next()
        val replicateUuid = l.split("\t")
        _uuidList += replicateUuid(1)
        _biologicalReplicateNumber += bioReplicateNumber
      }
    }
  })

  bioSampleList.BiosampleList.foreach(bioReplicateNumber => {
    val r = ("replicates__" + bioReplicateNumber + "__technical_replicate_number").r
    for(l <- lines){
      val mi = r.findAllIn(l)
      if(mi.hasNext) {
        mi.next()
        val replicateNumber = l.split("\t")
        //val replicateNumber = l.split("__", 3)
        _technicalReplicateNumberList += replicateNumber(1)
      }
    }
  })

  def UuidList: List[String] = _uuidList.toList

  def TechnicalReplicateNumberList: List[String]  = _technicalReplicateNumberList.toList

  def BiologicalReplicateNumberList: List[String] = _biologicalReplicateNumber.toList

}
