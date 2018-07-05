package it.polimi.genomics.importer.ModelDatabase.REP.Table

import it.polimi.genomics.importer.ModelDatabase.REP.REPTableId
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.ModelDatabase.{Dataset, Donor, Table}

class DatasetREP(repTableId: REPTableId) extends REPTable(repTableId) with Dataset {

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = {insertMethod(this.name,param)}
    case "DATATYPE" => this.dataType = {insertMethod(this.dataType,param)}
    case "FORMAT" => this.format = {insertMethod(this.format,param)}
    case "ASSEMBLY" => this.assembly = insertMethod(this.assembly,param)
    case "ISANN" => this.isAnn = if(insertMethod(this.isAnn.toString,param).equals("true")) true else false
    case "ANNOTATION" => this.annotation = insertMethod(this.annotation,param)
    case "LOCALURL" => this.localUrl = {insertMethod(this.localUrl,param)}
    case _ => noMatching(dest)
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    table match {
      case donor: Donor =>{
        var res = true
        val donorEncode = donor.asInstanceOf[DonorREP]
          donorEncode.speciesArray.filter(donorSpecies => donorSpecies != null).foreach(donorSpecies =>{
            if(donorSpecies.toUpperCase().equals("HOMO SAPIENS") && !(this.assembly.equals("hg19") || this.assembly.equals("GRCh38"))) {
              Statistics.constraintsViolated += 1
              this.logger.warn("Dataset species constraints violated")
              res = false
            }
          })
        res
      }
        case _ => true
    }
  }

  override def setForeignKeys(table: Table): Unit = {
  }

}
