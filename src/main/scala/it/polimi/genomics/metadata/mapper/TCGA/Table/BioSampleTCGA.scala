package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.BioSample

import scala.util.Try

class BioSampleTCGA extends TCGATable with BioSample {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = {
    dest.toUpperCase match {
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId, param)
      case "TYPES" => this.types = insertMethod(this.types, param)
      case "TISSUE" => {
        if (conf.getBoolean("import.rules.type")) {
          this.tissue = if (this.types.equals("tissue")) insertMethod(this.tissue, param) else null
        } else {
          this.tissue = insertMethod(this.tissue, param)
        }
      }
      case "CELL" => {
        if (conf.getBoolean("import.rules.type")) {
          if (this.types!=null && !this.types.equals("tissue"))
            this.cell = insertMethod(this.cell, param)
          else
            this.cell = null
        } else {
          this.cell = insertMethod(this.cell, param)
        }
      }
      case "ISHEALTHY" => {
        if (param.toUpperCase == "NORMAL" || param.toUpperCase == "HEALTHY")
          this.isHealthy = Some(true)
        else //if (param.toUpperCase == "TUMORAL")
          this.isHealthy = Some(false)
       // else
       //   this.isHealthy = None
        /*if (param == "null") this.isHealthy = false
        else
          param.toUpperCase match {
            case "ADDITIONAL - NEW PRIMARY" => this.isHealthy = false
            case "PRIMARY TUMOR" => this.isHealthy = false
            case "SOLID TIUSSE NORMAL" => this.isHealthy = true
            case "METASTATIC" => this.isHealthy = false
            case "BLOOD DERIVED NORMAL" => this.isHealthy = true
            case _ => logger.warn(s"Param ${param} not found in TCGA ISHEALTHY mapping")
          }*/
      }
      case "DISEASE" => {
        if (param.toUpperCase == "HEALTHY")
          this.disease = None
        else{
          val a: String = this.disease.getOrElse(null)
          val b: String = insertMethod(a,param)
          this.disease = Some(b)
        }
      }
      case _ => noMatching(dest)
    }
  }

}
