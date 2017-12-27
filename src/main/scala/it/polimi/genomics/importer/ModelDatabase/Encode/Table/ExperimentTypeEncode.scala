package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.ExperimentType

class ExperimentTypeEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with ExperimentType {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => this.feature = insertMethod(this.feature,param)
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case _ => noMatching(dest)

  }

}
