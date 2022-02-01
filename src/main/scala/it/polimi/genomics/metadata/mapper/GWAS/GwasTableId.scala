package it.polimi.genomics.metadata.mapper.GWAS

class GwasTableId {
  private var _ancestryArray: Array[String] = _
  private var _ancestryQuantity: Int = _

  def ancestryQuantity(value: Int) = _ancestryQuantity = value
  def ancestryQuantity = _ancestryQuantity
  def ancestryArray(array: Array[String]): Unit = _ancestryArray = array
}
