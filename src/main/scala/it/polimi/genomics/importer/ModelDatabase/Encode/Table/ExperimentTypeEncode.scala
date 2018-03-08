package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.ExperimentType

class ExperimentTypeEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with ExperimentType {

  var ontologicalCode: String = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => this.feature = insertMethod(this.feature,param)
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case "ONTOLOGICALCODE" => this.ontologicalCode = insertMethod(this.ontologicalCode,param)
    case _ => noMatching(dest)

  }

  override def insert(): Int = {
    val id: Int = super.insert()
    dbHandler.insertOntologyExperimentType(id, "experiment_type", "technique", "assay_term_name", "assay_term_id", ontologicalCode)
    id
  }

}
