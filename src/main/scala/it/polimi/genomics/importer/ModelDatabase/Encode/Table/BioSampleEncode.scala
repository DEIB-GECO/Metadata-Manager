package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.ModelDatabase.{BioSample, Table}

import scala.util.control.Breaks._

class BioSampleEncode(encodeTableId: EncodeTableId, quantity: Int) extends EncodeTable(encodeTableId) with BioSample {

  var donorIdArray: Array[Int] = new Array[Int](quantity)

  var sourceIdArray: Array[String] = new Array[String](quantity)

  var typesArray: Array[String] = new Array[String](quantity)

  var tissueArray: Array[String] = new Array[String](quantity)

  var cellLineArray: Array[String] = new Array[String](quantity)

  var isHealthyArray: Array[Boolean] = new Array[Boolean](quantity)

  var diseaseArray: Array[String] = new Array[String](quantity)

  var actualPosition: Int = _

  var insertPosition: Int = 0

  var typesInsertPosition: Int = 0

  var tissueInsertPosition: Int = 0

  var cellLineInsertPosition: Int = 0

  var isHealthyInsertPosition: Int = 0

  var diseaseInsertPosition: Int = 0


  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => {
        this.sourceIdArray(insertPosition) = insertMethod(this.sourceIdArray(insertPosition),param);
        this.insertPosition = resetPosition(this.insertPosition, quantity)
      }
      case "TYPES" => {
        this.typesArray(typesInsertPosition) = insertMethod(this.typesArray(typesInsertPosition),param);
        this.typesInsertPosition = resetPosition(this.typesInsertPosition, quantity);
      }
      case "TISSUE" => {
        this.tissueArray(tissueInsertPosition) = if(typesArray(tissueInsertPosition).equals("tissue")) insertMethod(this.tissueArray(tissueInsertPosition), param) else null
        this.tissueInsertPosition = resetPosition(this.tissueInsertPosition, quantity)
      }
      case "CELLLINE" => {
        this.cellLineArray(cellLineInsertPosition) = if(typesArray(cellLineInsertPosition).contains("cell")) insertMethod(this.cellLineArray(cellLineInsertPosition), param) else null;
        this.cellLineInsertPosition = resetPosition(this.cellLineInsertPosition, quantity)
      }
      case "ISHEALTHY" => {
        if(param.contains("healthy")) this.isHealthyArray(isHealthyInsertPosition) = true else this.isHealthyArray(isHealthyInsertPosition) = false
        this.isHealthyInsertPosition = resetPosition(isHealthyInsertPosition, quantity)
      }
      case "DISEASE" => {
        this.diseaseArray(diseaseInsertPosition) = if(!this.isHealthyArray(diseaseInsertPosition)) insertMethod(this.diseaseArray(diseaseInsertPosition),param) else null
        this.diseaseInsertPosition = resetPosition(diseaseInsertPosition, quantity)
      }
      case _ => noMatching(dest)
    }
  }



  override def nextPosition(globalKey: String, method: String): Unit = {
      globalKey.toUpperCase match {
        case "TYPES" => {
          this.typesInsertPosition = resetPosition(typesInsertPosition, quantity)
        }
        case "TISSUE" => {
          this.tissueInsertPosition = resetPosition(tissueInsertPosition, quantity)
        }
        case "CELLLINE" => {
          this.cellLineInsertPosition = resetPosition(cellLineInsertPosition, quantity)
        }
        case "ISHEALTHY" => {
          this.isHealthyInsertPosition = resetPosition(isHealthyInsertPosition, quantity)
        }
        case "DISEASE" => {
          this.diseaseInsertPosition = resetPosition(diseaseInsertPosition, quantity)
        }
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

    }
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    var res = true
    try {
      table match {
        case bioSamples: BioSampleEncode => {
          bioSamples.sourceIdArray.foreach(bioSample => {
            val position = bioSamples.sourceIdArray.indexOf(bioSample)
            if (bioSamples.typesArray(position).equals("tissue") && this.tissueArray(position) == null) {
              Statistics.constraintsViolated += 1
              this.logger.warn(s"Biosample tissue constrains violated ${_filePath}")
              res = false
            }
            else if (bioSamples.typesArray(position).contains("cell") && this.cellLineArray(position) == null) {
              Statistics.constraintsViolated += 1
              this.logger.warn("Biosample cellLine constrains violated")
              res = false
            }
            else if (bioSamples.isHealthyArray(position) && bioSamples.diseaseArray(position) != null) {
              Statistics.constraintsViolated += 1
              this.logger.warn("Biosample tissue constrains violated")
              res = false
            }
          })
          res
        }
        case _ => true
      }
    } catch {
      case e: Exception => {
        logger.warn("java.lang.NullPointerException")
        true
      };
    }
  }


  override def insert(): Int ={
    dbHandler.insertBioSample(donorIdArray(actualPosition),this.sourceIdArray(actualPosition),this.typesArray(actualPosition),this.tissueArray(actualPosition),this.cellLineArray(actualPosition),this.isHealthyArray(actualPosition),this.diseaseArray(actualPosition))
  }

  override def update(): Int = {
    dbHandler.updateBioSample(donorIdArray(actualPosition),this.sourceIdArray(actualPosition),this.typesArray(actualPosition),this.tissueArray(actualPosition),this.cellLineArray(actualPosition),this.isHealthyArray(actualPosition),this.diseaseArray(actualPosition))
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

  /*override def convertTo(values: Seq[(Int, String, Option[String], Option[String], Option[String], Boolean, Option[String])]): Unit = {
    values.foreach(value =>{
      this.donorIdArray(insertPosition) = value._1
      this.sourceIdArray(insertPosition) = value._2
      if(value._3.isDefined) this.typesArray(insertPosition) = value._3.get
      if(value._4.isDefined) this.tIssueArray(insertPosition) = value._4.get
      if(value._5.isDefined) this.cellLineArray(insertPosition) = value._5.get
      this.isHealtyArray(insertPosition) = value._6
      if(value._7.isDefined) this.diseaseArray(insertPosition) = value._7.get
    })

  }*/

}
