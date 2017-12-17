package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.BioSample

class BioSampleTCGA extends TCGATable with BioSample {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
      case "TYPES" => this.types = insertMethod(this.types,param)
      case "TISSUE" => this.tIssue = insertMethod(this.tIssue, param)
      case "CELLLINE" => this.cellLine = insertMethod(this.cellLine, param)
      case "ISHEALTY" => this.isHealty = false
      case "DISEASE" => this.disease = insertMethod(this.disease,param)
      case _ => noMatching(dest)
    }
  }

}
