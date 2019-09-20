package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.{Dataset, Table}

class DatasetTCGA extends TCGATable with Dataset{

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = insertMethod(this.name,param)
    case "DATATYPE" => insertMethod(this.dataType, param) match{
      case "cnv" => this.dataType = "cnv" //old tcga:cnv
      case "dnaseq" => this.dataType = "dna seq" //old tcga:dnaseq
      case "dnamethylation450"|"dnamethylation27" => this.dataType = "dna methylation" //old tcga:dnamethylation
      case _ => this.dataType = insertMethod(this.dataType, param)
    }
    case "FORMAT" => this.format = {insertMethod(this.format,param)}
    case "ASSEMBLY" => this.assembly = insertMethod(this.assembly,param)
    case "ISANN" => this.isAnn = if(insertMethod(this.isAnn.toString,param).equals("true")) true else false
    case _ => noMatching(dest)
  }


  override def setForeignKeys(table: Table): Unit = {
  }
}

