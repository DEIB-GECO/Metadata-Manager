package it.polimi.genomics.importer.ModelDatabase


class Donor extends EncodeTable{

  var donorId: Int = _

  var sourceId: String = _

  var species :String = _

  var age : Int = _

  var gender : String = _

  var ethnicity : String = _


  def setParameter(param: String, dest: String): Unit ={
    dest.toUpperCase() match {
      case "SOURCEID" => this.sourceId = setValue(this.sourceId, param)
      case "SPECIES" => this.species = setValue(this.species, param)
      case "AGE" => age = param.toInt
      case "GENDER" => this.gender = setValue(this.gender, param)
      case "ETHNICITY" => this.ethnicity = setValue(this.ethnicity, param)
      case _ => noMatching(dest)
    }
  }

 // override def setValue(actualParam: String, newParam: String): String = super.setValue(actualParam,newParam)
}
