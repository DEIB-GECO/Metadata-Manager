package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.{Dataset, Table}

class DatasetTCGA extends TCGATable with Dataset{

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = insertMethod(this.name,param)
    case "DATATYPE" => this.dataType = {insertMethod(this.dataType,param)}
    case "FORMAT" => this.format = {insertMethod(this.format,param)}
    case "ASSEMBLY" => this.assembly = insertMethod(this.assembly,param)
    case "ISANN" => this.isAnn = if(insertMethod(this.isAnn.toString,param).equals("true")) true else false
    case "ANNOTATION" => this.annotation = insertMethod(this.annotation,param)
    case _ => noMatching(dest)
  }


  override def setForeignKeys(table: Table): Unit = {
  }
}
