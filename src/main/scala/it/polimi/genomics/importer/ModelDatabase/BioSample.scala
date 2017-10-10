package it.polimi.genomics.importer.ModelDatabase

class BioSample extends EncodeTable{

  var donorId: Int = _

  var sourceId: String = _

  var types: String = _

  var tIssue: String = _

  var cellLine: String = _

  var isHealty: Boolean = _

  var disease: String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("DONORS")

  override def setParameter(param: String, dest: String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => this.sourceId = setValue(this.sourceId,param)
      case "TYPES" => this.types = setValue(this.types,param)
      case "TISSUE" => this.tIssue = if(types.equals("tissue")) setValue(this.tIssue, param) else null
      case "CELLLINE" => this.cellLine = if(types.equals("cell_line")) setValue(this.cellLine, param) else null
      case "ISHEALTY" => this.isHealty = if(param.contains("healthy")) true else false
      case "DISEASE" => if(this.isHealty) this.isHealty.toString else null
      case _ => noMatching(dest)
    }
  }

  override def insert(): Int ={
    dbHandler.insertBioSample(donorId,this.sourceId,this.types,this.tIssue,this.cellLine,this.isHealty,this.disease)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.donorId = table.getId
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertBioSample(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getBioSampleId(this.sourceId)
  }

}
