package it.polimi.genomics.metadata.mapper.Encode.Table

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.Utils.Statistics
import it.polimi.genomics.metadata.mapper.{Donor, Table}

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
        this.speciesArray(this.speciesInsertPosition) = insertMethod(this.speciesArray(this.speciesInsertPosition), param)
        this.speciesInsertPosition = resetPosition(speciesInsertPosition, quantity)
      }
      case "AGE" => { /*param.split(" ")(1).toUpperCase() match {
        case "YEAR" => {
          this.ageArray(this.ageInsertPosition) = param.split(" ")(0).toInt * 365
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
        }
        case "MONTH" => {
          this.ageArray(this.ageInsertPosition) = param.split(" ")(0).toInt * 30
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
        }
        case "DAY" => {
          this.ageArray(this.ageInsertPosition) = param.split(" ")(0).toInt
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)

        }
        case _ => {
          this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
        }*/
        this.ageArray(this.ageInsertPosition) = insertMethod(this.ageArray(this.ageInsertPosition).toString, param).toInt
        this.ageInsertPosition = resetPosition(ageInsertPosition, quantity)
      }
      case "GENDER" => {
        this.genderArray(this.genderInsertPosition) = insertMethod(this.genderArray(this.genderInsertPosition), param)
        this.genderInsertPosition = resetPosition(genderInsertPosition, quantity)
      }
      case "ETHNICITY" => {
        this.ethnicityArray(this.ethnicityInsertPosition) = insertMethod(this.ethnicityArray(this.ethnicityInsertPosition), param)
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

  /*override def insertRow(): Unit ={
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
  }*/

  override def insertRow(): Int ={
    var id: Int = 0
    for(sourcePosition <- 0 to sourceIdArray.length-1) {
      Statistics.donorInsertedOrUpdated += 1
      this.actualPosition = sourcePosition
      id = this.getId
      if (id == -1) {
        val newId = this.insert
        this.primaryKeys_(newId)
      } else {
        this.primaryKeys_(id)
        this.updateById()
      }
    }
    id
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    true
  }

    override def insert(): Int ={
    dbHandler.insertDonor(this.sourceIdArray(actualPosition),this.speciesArray(actualPosition),this.ageArray(actualPosition),this.genderArray(actualPosition),this.ethnicityArray(actualPosition))
  }

 // override def insert(states: collection.mutable.Map[String, String]): Int = ???

  override def update(): Int ={
    dbHandler.updateDonor(this.sourceIdArray(actualPosition),this.speciesArray(actualPosition),this.ageArray(actualPosition),this.genderArray(actualPosition),this.ethnicityArray(actualPosition))
  }

  override def updateById(): Unit ={
    dbHandler.updateDonorById(this.primaryKeys(actualPosition), this.sourceIdArray(actualPosition),this.speciesArray(actualPosition),this.ageArray(actualPosition),this.genderArray(actualPosition),this.ethnicityArray(actualPosition))
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

  override def writeInFile(path: String, biologicalReplicateNum: String = ""): Unit = {
    val write = getWriter(path)
    val tableName = "donor"

    write.append(getMessageMultipleAttribute(this.sourceId, tableName, biologicalReplicateNum, "donor_source_id"))
    if(this.species != null) write.append(getMessageMultipleAttribute(this.species, tableName, biologicalReplicateNum, "species"))
    if(this.age != 0) write.append(getMessageMultipleAttribute(this.age, tableName, biologicalReplicateNum, "age"))
    if(this.gender != null) write.append(getMessageMultipleAttribute(this.gender, tableName, biologicalReplicateNum, "gender"))
    if(this.ethnicity != null) write.append(getMessageMultipleAttribute(this.ethnicity, tableName, biologicalReplicateNum, "ethnicity"))

    flushAndClose(write)
  }

}
