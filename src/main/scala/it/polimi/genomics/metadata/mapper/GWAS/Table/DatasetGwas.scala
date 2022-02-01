package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import it.polimi.genomics.metadata.mapper.{Dataset, Table}

class DatasetGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId)  with Dataset {
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = {insertMethod(this.name,param)}
    case "DATATYPE" =>
      this.dataType = {insertMethod(this.dataType,param)}
    case "FORMAT" =>
      this.format = {insertMethod(this.format,param)}
    case "ASSEMBLY" => this.assembly = insertMethod(this.assembly,param)
    case "ISANN" => this.isAnn = if(insertMethod(this.isAnn.toString,param).equals("true")) true else false
    case _ => noMatching(dest)
  }

  override def setForeignKeys(table: Table): Unit = ???
}
