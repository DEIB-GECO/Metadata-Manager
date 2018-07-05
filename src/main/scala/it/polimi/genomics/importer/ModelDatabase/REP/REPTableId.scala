package it.polimi.genomics.importer.ModelDatabase.REP

import scala.collection.mutable

class REPTableId {

  private var _caseId: Int = _
  private var _replicateMap: mutable.Map[String, Int] = collection.mutable.Map[String, Int]()
  private var _biosampleArray: Array[String] = _
  private var _bioSampleQuantity: Int = _
  private var _techReplicateArray: Array[String] = _

  def bioSampleQuantity = _bioSampleQuantity
  def bioSampleQuantity(value: Int) = _bioSampleQuantity = value

  def bioSampleArray(array: Array[String]): Unit = _biosampleArray = array

  def setQuantityTechReplicate(quantity: Int): Unit = _techReplicateArray = new Array[String](quantity)
  def techReplicateArray: Array[String] = _techReplicateArray
  def techReplicateArray(array: Array[String]): Unit = _techReplicateArray = array

  def caseId_ (value:Int):Unit = _caseId = value

  def replicateMap_(key: String, value: Int): Unit = _replicateMap += key -> value

}
