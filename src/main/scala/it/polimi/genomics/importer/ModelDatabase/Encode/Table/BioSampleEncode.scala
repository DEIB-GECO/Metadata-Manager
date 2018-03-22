package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.ModelDatabase.{BioSample, Table}
import it.polimi.genomics.importer.RemoteDatabase.DbHandler

import scala.util.control.Breaks._

class BioSampleEncode(encodeTableId: EncodeTableId, quantity: Int) extends EncodeTable(encodeTableId) with BioSample {

  var donorIdArray: Array[Int] = new Array[Int](quantity)

  var sourceIdArray: Array[String] = new Array[String](quantity)

  var typesArray: Array[String] = new Array[String](quantity)

  var tissueArray: Array[String] = new Array[String](quantity)

  var cellLineArray: Array[String] = new Array[String](quantity)

  var isHealthyArray: Array[Boolean] = new Array[Boolean](quantity)

  var diseaseArray: Array[String] = new Array[String](quantity)

  var ontologicalCode: Array[String] = new Array[String](quantity)

  var originalKey: Array[String] = new Array[String](quantity)

  //var originalValue: Array[String] = new Array[String](quantity)

  var actualPosition: Int = _

  var insertPosition: Int = 0

  var typesInsertPosition: Int = 0

  var tissueInsertPosition: Int = 0

  var cellLineInsertPosition: Int = 0

  var isHealthyInsertPosition: Int = 0

  var diseaseInsertPosition: Int = 0

  var ontologicalCodePosition: Int = 0

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
        if(conf.getBoolean("import.rules.type")) {
          this.tissueArray(tissueInsertPosition) = if (typesArray(tissueInsertPosition).equals("tissue")) insertMethod(this.tissueArray(tissueInsertPosition), param) else null
        } else {
          this.tissueArray(tissueInsertPosition) = insertMethod(this.tissueArray(tissueInsertPosition), param)
        }
        this.tissueInsertPosition = resetPosition(this.tissueInsertPosition, quantity)
      }
      case "CELLLINE" => {
        if(conf.getBoolean("import.rules.type")) {
          this.cellLineArray(cellLineInsertPosition) = if (typesArray(cellLineInsertPosition).contains("cell")) insertMethod(this.cellLineArray(cellLineInsertPosition), param) else null
        }
        else{
          this.cellLineArray(cellLineInsertPosition) =insertMethod(this.cellLineArray(cellLineInsertPosition), param)
        }
        this.cellLineInsertPosition = resetPosition(this.cellLineInsertPosition, quantity)
      }
      case "ISHEALTHY" => {
        if(param.contains("healthy")) this.isHealthyArray(isHealthyInsertPosition) = true else this.isHealthyArray(isHealthyInsertPosition) = false
        this.isHealthyInsertPosition = resetPosition(isHealthyInsertPosition, quantity)
      }
      case "DISEASE" => {
        if(conf.getBoolean("import.rules.is_healthy")) {
          this.diseaseArray(diseaseInsertPosition) = if (!this.isHealthyArray(diseaseInsertPosition)) insertMethod(this.diseaseArray(diseaseInsertPosition), param) else null
        } else {
          this.diseaseArray(diseaseInsertPosition) = insertMethod(this.diseaseArray(diseaseInsertPosition), param)
        }
        this.diseaseInsertPosition = resetPosition(diseaseInsertPosition, quantity)
      }
      case "ONTOLOGICALCODE" => {
        this.ontologicalCode(ontologicalCodePosition) = insertMethod(this.ontologicalCode(ontologicalCodePosition),param)
        this.ontologicalCodePosition = resetPosition(ontologicalCodePosition, quantity)
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
        case "ONTOLOGICALCODE" => {
          this.ontologicalCodePosition = resetPosition(ontologicalCodePosition, quantity)
        }
      }
  }

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


  override def insert(): Int = {
    val id = dbHandler.insertBioSample(donorIdArray(actualPosition),this.sourceIdArray(actualPosition),this.typesArray(actualPosition),this.tissueArray(actualPosition),this.cellLineArray(actualPosition),this.isHealthyArray(actualPosition),this.diseaseArray(actualPosition))
    if(conf.getBoolean("import.support_table_insert"))
      insertOrUpdateOntologicTuple(id)
     /* if(this.cellLineArray(actualPosition) != null && conf.getBoolean("import.support_table_insert"))
      dbHandler.insertOntology(id, "biosample", "cell_line", ontologicalCode(actualPosition).split('*')(0), this.cellLineArray(actualPosition), ontologicalCode(actualPosition).split('*')(1))
    if(this.tissueArray(actualPosition) != null && conf.getBoolean("import.support_table_insert"))
      dbHandler.insertOntology(id, "biosample", "tissue", ontologicalCode(actualPosition).split('*')(0), this.tissueArray(actualPosition), ontologicalCode(actualPosition).split('*')(1))*/
    id
  }

  override def update(): Int = {
    val id = dbHandler.updateBioSample(donorIdArray(actualPosition),this.sourceIdArray(actualPosition),this.typesArray(actualPosition),this.tissueArray(actualPosition),this.cellLineArray(actualPosition),this.isHealthyArray(actualPosition),this.diseaseArray(actualPosition))
    if(conf.getBoolean("import.support_table_insert"))
      insertOrUpdateOntologicTuple(id)
    id
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

  override def writeInFile(path: String, biologicalReplicateNum: String = ""): Unit = {
    val write = getWriter(path)
    val tableName = "biosample"
    write.append(getMessageMultipleAttribute(this.sourceId, tableName, biologicalReplicateNum, "source_id"))
    if(this.types != null) write.append(getMessageMultipleAttribute(this.types, tableName, biologicalReplicateNum, "type"))
    if(this.tissue != null) write.append(getMessageMultipleAttribute(this.tissue, tableName, biologicalReplicateNum, "tissue"))
    if(this.cellLine != null) write.append(getMessageMultipleAttribute(this.cellLine, tableName, biologicalReplicateNum, "cell_line"))
    write.append(getMessageMultipleAttribute(this.isHealthy, tableName, biologicalReplicateNum, "is_healthy"))
    if(this.disease != null) write.append(getMessageMultipleAttribute( this.disease, tableName, biologicalReplicateNum, "disease"))
    flushAndClose(write)
  }

  def insertOrUpdateOntologicTuple(id: Int): Unit = {
    if(this.cellLineArray(actualPosition) != null)
      if(DbHandler.checkInsertOntology(id, "biosample", "cell_line"))
        dbHandler.insertOntology(id, "biosample", "cell_line", ontologicalCode(actualPosition).split('*')(0), this.cellLineArray(actualPosition), ontologicalCode(actualPosition).split('*')(1))
      else
        dbHandler.updateOntology(id, "biosample", "cell_line", ontologicalCode(actualPosition).split('*')(0), this.cellLineArray(actualPosition), ontologicalCode(actualPosition).split('*')(1))
    if(this.tissueArray(actualPosition) != null)
      if(DbHandler.checkInsertOntology(id, "biosample", "tissue"))
        dbHandler.insertOntology(id, "biosample", "tissue", ontologicalCode(actualPosition).split('*')(0), this.tissueArray(actualPosition), ontologicalCode(actualPosition).split('*')(1))
      else
        dbHandler.updateOntology(id, "biosample", "tissue", ontologicalCode(actualPosition).split('*')(0), this.tissueArray(actualPosition), ontologicalCode(actualPosition).split('*')(1))
  }
}
