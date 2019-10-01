package it.polimi.genomics.metadata.mapper.REP.Table

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.ExperimentType
import it.polimi.genomics.metadata.mapper.REP.REPTableId

class ExperimentTypeREP(repTableId: REPTableId) extends REPTable(repTableId) with ExperimentType {

  var ontologicalCode: String = _

  var originalKey: String = _

  //var originalValue: String = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => param match {
      case "DMR" => this.technique = insertMethod(this.technique, param)
      case "RRBS" => this.technique = "RRBS"
      case "WGBS" => this.technique = "WGBS"
      case _ => this.technique = insertMethod(this.technique, param)
    }
    case "FEATURE" => this.feature = insertMethod(this.feature,param)

    case "TARGET" => param.toUpperCase() match {
      case "DNASE" => this.target = null
      case _ => this.target = insertMethod(this.target,param)
    }

    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case "ONTOLOGICALCODE" => this.ontologicalCode = insertMethod(this.ontologicalCode,param)
    case "ORIGINALKEY" => this.originalKey = insertMethod(this.originalKey, param)
    case _ => noMatching(dest)

  }

}

