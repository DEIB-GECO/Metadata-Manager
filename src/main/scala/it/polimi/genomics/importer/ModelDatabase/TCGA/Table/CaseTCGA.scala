package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Case

class CaseTCGA extends TCGATable with Case{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SOURCESITE" => this.sourceSite = insertMethod(this.sourceSite,param)
    case "EXTERNALREF" => this.externalRef =  insertMethod(this.externalRef,param)
    case _ => noMatching(dest)
  }

}
