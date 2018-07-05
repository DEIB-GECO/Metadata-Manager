package it.polimi.genomics.importer.ModelDatabase.REP.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.ExperimentType
import it.polimi.genomics.importer.ModelDatabase.REP.REPTableId

class ExperimentTypeREP(repTableId: REPTableId) extends REPTable(repTableId) with ExperimentType {

  var ontologicalCode: String = _

  var originalKey: String = _

  //var originalValue: String = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => this.feature = insertMethod(this.feature,param)
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case "ONTOLOGICALCODE" => this.ontologicalCode = insertMethod(this.ontologicalCode,param)
    case "ORIGINALKEY" => this.originalKey = insertMethod(this.originalKey, param)
    case _ => noMatching(dest)

  }

}
