package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import it.polimi.genomics.metadata.mapper.{Cohort}


class CohortGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId)  with Cohort{
  override def getId: Int = ???

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String):  Unit =   dest.toUpperCase() match{
    case "TRAITNAME" => this.traitName = {insertMethod(this.traitName,param)}
    case "CASENUMBER_INITIAL" => this.caseNumber_initial = insertMethod(this.caseNumber_initial.toString,param).toInt
    case "CONTROLNUMBER_INITIAL" => this.controlNumber_initial = insertMethod(this.controlNumber_initial.toString,param).toInt
    case "INDIVIDUALNUMBER_INITIAL" => this.individualNumber_initial = insertMethod(this.individualNumber_initial.toString,param).toInt
    case "TRIOSNUMBER_INITIAL" => this.triosNumber_initial = insertMethod(this.triosNumber_initial.toString,param).toInt
    case "CASENUMBER_REPLICATE" => this.caseNumber_replicate = insertMethod(this.caseNumber_replicate.toString,param).toInt
    case "CONTROLNUMBER_REPLICATE" => this.controlNumber_replicate = insertMethod(this.controlNumber_replicate.toString,param).toInt
    case "INDIVIDUALNUMBER_REPLICATE" => this.individualNumber_replicate = insertMethod(this.individualNumber_replicate.toString,param).toInt
    case "TRIOSNUMBER_REPLICATE" => this.triosNumber_replicate = insertMethod(this.triosNumber_replicate.toString,param).toInt
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case _ => noMatching(dest)
  }

}
