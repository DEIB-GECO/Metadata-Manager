package it.polimi.genomics.importer.ModelDatabase

class Case extends EncodeTable{

    var projectId : Int = _

    var sourceId : String = _

    var sourceSite : String = _

  override def setParameter(param: String, dest: String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = setValue(this.sourceId,param)
    case "SOURCESITE" => this.sourceSite = setValue(this.sourceSite,param)
    case _ => noMatching(dest)
  }
}
