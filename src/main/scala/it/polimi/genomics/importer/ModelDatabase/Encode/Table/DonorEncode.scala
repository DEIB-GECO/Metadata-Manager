package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.{Donor, Table}
import it.polimi.genomics.importer.RemoteDatabase.DbHandler

import scala.util.control.Breaks.{break, breakable}

class DonorEncode(encodeTableId: EncodeTableId, quantity: Int) extends EncodeTable(encodeTableId) with Donor{

  val sourceIdArray: Array[String] = new Array[String](quantity)

  var speciesArray: Array[String] = new Array[String](quantity)

  var ageArray: Array[Int] = new Array[Int](quantity)

  var genderArray: Array[String] = new Array[String](quantity)

  var ethnicityArray: Array[String] = new Array[String](quantity)

  var actualPosition: Int = _

  var insertPosition: Int = -1

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit ={
    dest.toUpperCase() match {
      case "SOURCEID" =>{this.insertPosition += 1 ; this.sourceIdArray(this.insertPosition) = insertMethod(this.sourceIdArray(this.insertPosition), param) }
      case "SPECIES" => this.speciesArray(this.insertPosition) = insertMethod(this.speciesArray(this.insertPosition), param)
      case "AGE" => ageArray(this.insertPosition) = param.toInt
      case "GENDER" => this.genderArray(this.insertPosition) = insertMethod(this.genderArray(this.insertPosition), param)
      case "ETHNICITY" => this.ethnicityArray(this.insertPosition) = insertMethod(this.ethnicityArray(this.insertPosition), param)
      case _ => noMatching(dest)
    }
  }

  override def insertRow(): Unit ={
    var id: Int = 0
    for(sourcePosition <- 0 to sourceIdArray.length-1){
      this.actualPosition = sourcePosition
      if(this.checkInsert()) {
        id = this.insert
      }
      else {
        id = this.update
      }
      this.primaryKeys_(id)
    }
  }

  override def insert(): Int ={
    dbHandler.insertDonor(this.sourceIdArray(actualPosition),this.speciesArray(actualPosition),this.ageArray(actualPosition),this.genderArray(actualPosition),this.ethnicityArray(actualPosition))
  }

  override def update(): Int ={
    dbHandler.updateDonor(this.sourceIdArray(actualPosition),this.speciesArray(actualPosition),this.ageArray(actualPosition),this.genderArray(actualPosition),this.ethnicityArray(actualPosition))
  }

  override def setForeignKeys(table: Table): Unit = {
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertDonor(this.sourceIdArray(actualPosition))
  }

  override def checkConsistency(): Boolean = {
    this.sourceIdArray.foreach(source => if(this.sourceIdArray == null) false)
    true
  }

  override def getId(): Int = {
    dbHandler.getDonorId(this.sourceIdArray(actualPosition))
  }


}
