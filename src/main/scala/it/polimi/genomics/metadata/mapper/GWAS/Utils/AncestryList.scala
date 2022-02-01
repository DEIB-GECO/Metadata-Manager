
package it.polimi.genomics.metadata.mapper.GWAS.Utils

import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import scala.collection.mutable.ListBuffer

class AncestryList (lines: Array[String], gwasTableId: GwasTableId) {

  val reg = "stage_"

  private var _list = new ListBuffer[String]()

  for(l <- lines) {
    if (l contains(reg)){
      _list += l.split("_")(1).split("\t")(0)
    }
  }

  gwasTableId.ancestryArray(_list.toArray)

  def ancestryQuantity = _list.length
  def ancestryList: List[String] = _list.toList

}
