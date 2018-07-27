package it.polimi.genomics.metadata.mapper.REP.Utils

import scala.collection.mutable.ListBuffer

class ReplicateList(lines: Array[String], bioSampleList: BioSampleList) {

  private var _uuidList = new ListBuffer[String]()
  private var _technicalReplicateNumberList = new ListBuffer[String]()
  private var _biologicalReplicateNumber = new ListBuffer[String]()


  bioSampleList.BiosampleList.foreach(bioReplicateNumber => {
    val r = "epi__sample_alias__" + bioReplicateNumber
    for (l <- lines) {
      val conf = l.split("\t")
      if (conf(0) == r) {
        _uuidList += conf(1)
        _biologicalReplicateNumber += bioReplicateNumber
        _technicalReplicateNumberList += "1"
      }
    }
  })

  def UuidList: List[String] = _uuidList.toList

  def TechnicalReplicateNumberList: List[String] = _technicalReplicateNumberList.toList

  def BiologicalReplicateNumberList: List[String] = _biologicalReplicateNumber.toList

}
