package it.polimi.genomics.importer.ModelDatabase


class Replicate extends EncodeTable{

  var experimentTypeId : Int = _

  var technique : String = _

  var feature : String = _

  var platform : String = _

  var target : String = _

  var antibody : String = _

  override def setParameter(param: String, dest: String): Unit = {
    dest.toUpperCase match{
      case "TECHNIQUE" => this.technique = setValue(this.technique,param)
      case "FEATURE" => this.feature = setValue(this.feature,param)
      case "PLATFORM" => this.platform = setValue(this.platform, param)
      case "TARGET" => this.target = setValue(this.target, param)
      case "ANTIBODY" => this.antibody = setValue(this.antibody, param)
      case _ => noMatching(dest)
    }
  }
}
