package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.BioSample

class BioSampleTCGA extends TCGATable with BioSample {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
      case "TYPES" => this.types = insertMethod(this.types,param)
      case "TISSUE" => this.tissue = insertMethod(this.tissue, param)
      case "CELLLINE" => this.cellLine = insertMethod(this.cellLine, param)
      case "ISHEALTY" => { param match {
        case "Additional - New Primary" => this.isHealty = false
        case "Primary Tumor" => this.isHealty = false
        case "Solid Tiusse Normal" => this.isHealty = true
        case "Metastatic" => this.isHealty = false
        case "Blood Derived Normal" => this.isHealty = true
      }}
      case "DISEASE" => if(!this.isHealty) this.disease = insertMethod(this.disease,param)
      case _ => noMatching(dest)
    }
  }

}
