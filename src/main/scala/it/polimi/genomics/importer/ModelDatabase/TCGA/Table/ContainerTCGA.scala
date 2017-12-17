package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Container

class ContainerTCGA extends TCGATable with Container{

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = insertMethod(this.name,param)
    case "ASSEMBLY" => this.assembly = insertMethod(this.assembly,param)
    case "ISANN" => this.isAnn = if(insertMethod(this.isAnn.toString,param).equals("true")) true else false
    case "ANNOTATION" => this.annotation = insertMethod(this.annotation,param)
    case _ => noMatching(dest)
  }

}
