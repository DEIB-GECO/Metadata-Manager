package it.polimi.genomics.metadata.mapper.Encode

import scala.collection.mutable

class EncodeTableId {

  private var _caseId: Int = _
  private var _replicateMap: mutable.Map[String, Int] = collection.mutable.Map[String, Int]()
  private var _biosampleArray: Array[String] = _
  private var _bioSampleQuantity: Int = _
  private var _techReplicateArray: Array[String] = _

  //private var bioSampleMap: mutable.Map[String, Int] = collection.mutable.Map[String, Int]()

  def bioSampleQuantity = _bioSampleQuantity
  def bioSampleQuantity(value: Int) = _bioSampleQuantity = value
 // def setQuantityBiosample(quantity: Int): Unit = _biosampleArray = new Array[String](quantity)

 // def bioSampleArray(pos: Int): String = _biosampleArray(pos)
 // def bioSampleArray(value: String, pos: Int) = _biosampleArray(pos) = value
  def bioSampleArray(array: Array[String]): Unit = _biosampleArray = array

  def setQuantityTechReplicate(quantity: Int): Unit = _techReplicateArray = new Array[String](quantity)
  def techReplicateArray: Array[String] = _techReplicateArray
  def techReplicateArray(array: Array[String]): Unit = _techReplicateArray = array
  // def techReplicateArrayByPos(pos: Int): String = _techReplicateArray(pos)


  //def setBiosampleIdMapping(position: Int, value: Int): Unit = bioSampleMap += _biosampleArray(position) -> value
  //def getBiosampleIdMapping(key: String): String = bioSampleMap(key)


 // def caseId: Int = _caseId
  def caseId_ (value:Int):Unit = _caseId = value

 // def replicateMap(value: String): Int = _replicateMap(value)
  def replicateMap_(key: String, value: Int): Unit = _replicateMap += key -> value

}
