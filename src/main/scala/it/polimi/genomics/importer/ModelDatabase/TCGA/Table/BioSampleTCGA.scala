package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.BioSample

class BioSampleTCGA extends TCGATable with BioSample {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
      case "TYPES" => this.types = insertMethod(this.types,param)
      case "TISSUE" => this.tissue = insertMethod(this.tissue, param)
      case "CELLLINE" => this.cellLine = insertMethod(this.cellLine, param)
      case "ISHEALTHY" => { param.toUpperCase match {
        case "ADDITIONAL - NEW PRIMARY" => this.isHealthy = false
        case "PRIMARY TUMOR" => this.isHealthy = false
        case "SOLID TIUSSE NORMAL" => this.isHealthy = true
        case "METASTATIC" => this.isHealthy = false
        case "BLOOD DERIVED NORMAL" => this.isHealthy = true
        case _ => logger.warn(s"Param ${param} not found in TCGA ISHEALTHY mapping")
      }}
      case "DISEASE" => this.disease = insertMethod(this.disease,param)
      case _ => noMatching(dest)
    }
  }

}
