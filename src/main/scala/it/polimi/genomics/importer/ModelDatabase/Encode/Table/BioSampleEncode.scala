package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.{BioSample, Table}

import scala.util.control.Breaks._

class BioSampleEncode(encodeTableId: EncodeTableId, quantity: Int) extends EncodeTable(encodeTableId) with BioSample {

  var donorIdArray: Array[Int] = new Array[Int](quantity)

  var sourceIdArray: Array[String] = new Array[String](quantity)

  var typesArray: Array[String] = new Array[String](quantity)

  var tIssueArray: Array[String] = new Array[String](quantity)

  var cellLineArray: Array[String] = new Array[String](quantity)

  var isHealtyArray: Array[Boolean] = new Array[Boolean](quantity)

  var diseaseArray: Array[String] = new Array[String](quantity)

  var actualPosition: Int = _

  var insertPosition: Int = -1

  _hasForeignKeys = true

  _foreignKeysTables = List("DONORS")


  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => {this.insertPosition += 1;this.sourceIdArray(insertPosition) = insertMethod(this.sourceIdArray(insertPosition),param);}
      case "TYPES" => this.typesArray(insertPosition) = insertMethod(this.typesArray(insertPosition),param)
      case "TISSUE" => this.tIssueArray(insertPosition) = if(typesArray(insertPosition) .equals("tissue")) insertMethod(this.tIssueArray(insertPosition), param) else null
      case "CELLLINE" => this.cellLineArray(insertPosition) = if(typesArray(insertPosition) .contains("cell")) insertMethod(this.cellLineArray(insertPosition), param) else null
      case "ISHEALTY" => this.isHealtyArray(insertPosition) = if(param.contains("healthy")) true else false
      case "DISEASE" => this.diseaseArray(insertPosition) = if(this.isHealtyArray(insertPosition)) insertMethod(this.diseaseArray(insertPosition),param) else null
      case _ => noMatching(dest)
    }
  }

 /* def setQuantity(quantity: Int): Unit ={
    sourceId = new Array[String](quantity)
    types = new Array[String](quantity)
    tIssue = new Array[String](quantity)
    cellLine = new Array[String](quantity)
    isHealty = new Array[Boolean](quantity)
    disease = new Array[String](quantity)
  }*/

  override def insertRow(): Unit ={
    var id: Int = 0
    var position = 0
    val array = this.encodeTableId.techReplicateArray
    for(sourcePosition <- 0 to sourceIdArray.length-1){
      this.actualPosition = sourcePosition
      if(this.checkInsert()) {
        id = this.insert
      }
      else {
        id = this.update
      }
      val actualVal = array(position)
      /*breakable f{
        println("Biosample Id " + id)
        this.primaryKeys_(id)
        position += 1
      }while(array(position) == actualVal && position < array.length)*/

      breakable { while(array(position) == actualVal && position < array.length) {
          this.primaryKeys_(id)
          position += 1
          if (position >= array.length) break
        } }
      /*def setPrimaryKey(position:Unit)={
        if()*/

    }
  }


  override def insert(): Int ={
    dbHandler.insertBioSample(donorIdArray(actualPosition),this.sourceIdArray(actualPosition),this.typesArray(actualPosition),this.tIssueArray(actualPosition),this.cellLineArray(actualPosition),this.isHealtyArray(actualPosition),this.diseaseArray(actualPosition))
  }

  override def update(): Int = {
    dbHandler.updateBioSample(donorIdArray(actualPosition),this.sourceIdArray(actualPosition),this.typesArray(actualPosition),this.tIssueArray(actualPosition),this.cellLineArray(actualPosition),this.isHealtyArray(actualPosition),this.diseaseArray(actualPosition))
  }

  override def setForeignKeys(table: Table): Unit = {
    this.donorIdArray = table.primaryKeys.toArray
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertBioSample(this.sourceIdArray(actualPosition))
  }

  override def getId(): Int = {
    dbHandler.getBioSampleId(this.sourceIdArray(actualPosition))
  }

  override def checkConsistency(): Boolean = {
    this.sourceIdArray.foreach(source => if(source == null) false)
    true
  }

}
