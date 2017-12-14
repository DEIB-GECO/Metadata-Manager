package it.polimi.genomics.importer.ModelDatabase

import scala.util.control.Breaks._

class BioSampleEncode(encodeTableId: EncodeTableId, quantity: Int) extends EncodeTable(encodeTableId){

  var donorId: Array[Int] = new Array[Int](quantity)

  var sourceId: Array[String] = new Array[String](quantity)

  var types: Array[String] = new Array[String](quantity)

  var tIssue: Array[String] = new Array[String](quantity)

  var cellLine: Array[String] = new Array[String](quantity)

  var isHealty: Array[Boolean] = new Array[Boolean](quantity)

  var disease: Array[String] = new Array[String](quantity)

  var actualPosition: Int = _

  var insertPosition: Int = -1

  _hasForeignKeys = true

  _foreignKeysTables = List("DONORS")


  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => {this.insertPosition += 1;this.sourceId(insertPosition) = insertMethod(this.sourceId(insertPosition),param);}
      case "TYPES" => this.types(insertPosition) = insertMethod(this.types(insertPosition),param)
      case "TISSUE" => this.tIssue(insertPosition) = if(types(insertPosition) .equals("tissue")) insertMethod(this.tIssue(insertPosition), param) else null
      case "CELLLINE" => this.cellLine(insertPosition) = if(types(insertPosition) .contains("cell")) insertMethod(this.cellLine(insertPosition), param) else null
      case "ISHEALTY" => this.isHealty(insertPosition) = if(param.contains("healthy")) true else false
      case "DISEASE" => this.disease(insertPosition) = if(this.isHealty(insertPosition)) insertMethod(this.disease(insertPosition),param) else null
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
    for(sourcePosition <- 0 to sourceId.length-1){
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
          println("Biosample Id " + id)
          this.primaryKeys_(id)
          position += 1
          if (position >= array.length) break
        } }
      /*def setPrimaryKey(position:Unit)={
        if()*/

    }
  }


  override def insert(): Int ={
    dbHandler.insertBioSample(donorId(actualPosition),this.sourceId(actualPosition),this.types(actualPosition),this.tIssue(actualPosition),this.cellLine(actualPosition),this.isHealty(actualPosition),this.disease(actualPosition))
  }

  override def update(): Int = {
    dbHandler.updateBioSample(donorId(actualPosition),this.sourceId(actualPosition),this.types(actualPosition),this.tIssue(actualPosition),this.cellLine(actualPosition),this.isHealty(actualPosition),this.disease(actualPosition))
  }

  override def setForeignKeys(table: Table): Unit = {
    this.donorId = table.primaryKeys.toArray
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertBioSample(this.sourceId(actualPosition))
  }

  override def getId(): Int = {
    dbHandler.getBioSampleId(this.sourceId(actualPosition))
  }

  override def checkConsistency(): Boolean = {
    this.sourceId.foreach(source => if(this.sourceId == null) false)
    true
  }

}
