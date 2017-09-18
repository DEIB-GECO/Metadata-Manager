package it.polimi.genomics.importer.ModelDatabase


class BioSample extends EncodeTable{

  var donorId: Int = _

  var sourceId: String = _

  var types: String = _

  var tIssue: String = _

  var cellLine: String = _

  var isHealty: Boolean = _

  var disease: String = _


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


}
