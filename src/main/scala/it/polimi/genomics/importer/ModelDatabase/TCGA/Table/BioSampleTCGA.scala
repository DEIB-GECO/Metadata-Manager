package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.BioSample

class BioSampleTCGA extends TCGATable with BioSample {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
      case "TYPES" => this.types = insertMethod(this.types,param)
      case "TISSUE" => this.tissue = insertMethod(this.tissue, param)
      case "CELLLINE" => this.cellLine = insertMethod(this.cellLine, param)
      case "ISHEALTHY" => { param match {
        case "Additional - New Primary" => this.isHealthy = false
        case "Primary Tumor" => this.isHealthy = false
        case "Solid Tiusse Normal" => this.isHealthy = true
        case "Metastatic" => this.isHealthy = false
        case "Blood Derived Normal" => this.isHealthy = true
      }}
      case "DISEASE" => this.disease = insertMethod(this.disease,param)
      case _ => noMatching(dest)
    }
  }

}
