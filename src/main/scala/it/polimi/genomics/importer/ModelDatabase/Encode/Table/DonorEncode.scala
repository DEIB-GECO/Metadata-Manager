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

  var insertPosition: Int = 0

  var speciesInsertPosition: Int = 0

  var ageInsertPosition: Int = 0

  var genderInsertPosition: Int = 0

  var ethnicityInsertPosition: Int = 0

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit ={
    dest.toUpperCase() match {
      case "SOURCEID" =>{
        this.sourceIdArray(this.insertPosition) = insertMethod(this.sourceIdArray(this.insertPosition), param)
        this.insertPosition = resetPosition(insertPosition, quantity)
      }
      case "SPECIES" => {
        this.speciesArray(this.insertPosition) = insertMethod(this.speciesArray(this.insertPosition), param)
        this.speciesInsertPosition = resetPosition(speciesInsertPosition, quantity)
      }
      case "AGE" => param.split(" ")(1).toUpperCase() match {
        case "YEAR" => {
          this.ageArray(this.insertPosition) = param.split(" ")(0).toInt * 365
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
        }
        case "MONTH" => {
          this.ageArray(this.insertPosition) = param.split(" ")(0).toInt * 30
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
        }
        case "DAY" => {
          this.ageArray(this.insertPosition) = param.split(" ")(0).toInt
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)

        }
        case _ => {
          this.ageArray(this.insertPosition) = 0
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
        }
      }
      case "GENDER" => {
        this.genderArray(this.insertPosition) = insertMethod(this.genderArray(this.insertPosition), param)
        this.genderInsertPosition = resetPosition(genderInsertPosition, quantity)
      }
      case "ETHNICITY" => {
        this.ethnicityArray(this.insertPosition) = insertMethod(this.ethnicityArray(this.insertPosition), param)
        this.ethnicityInsertPosition = resetPosition(ethnicityInsertPosition, quantity)
      }
      case _ => noMatching(dest)
    }
  }

  override def nextPosition(globalKey: String, method: String): Unit = {
    globalKey.toUpperCase match {
      case "SPECIES" => {
        this.speciesInsertPosition = resetPosition(speciesInsertPosition, quantity)
      }
      case "AGE" => {
        this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
      }
      case "GENDER" => {
        this.genderInsertPosition = resetPosition(genderInsertPosition, quantity)
      }
      case "ETHNICITY" => {
        this.ethnicityInsertPosition = resetPosition(ethnicityInsertPosition, quantity)
      }
    }
  }

  override def noMatching(message: String): Unit = super.noMatching(message)

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

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    true
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
