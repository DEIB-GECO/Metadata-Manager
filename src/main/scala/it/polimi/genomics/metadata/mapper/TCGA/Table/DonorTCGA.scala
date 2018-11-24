package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.Donor

class DonorTCGA extends TCGATable with Donor{

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit ={
    dest.toUpperCase() match {
      case "SOURCEID" =>
        this.sourceId = insertMethod(this.sourceId, param)
      case "SPECIES" => this.species = insertMethod(this.species, param)
      case "AGE" =>
        this.age = insertMethod(this.age.toString,param).toInt //param.toInt.abs
      case "GENDER" => this.gender = insertMethod(this.gender, param)
      case "ETHNICITY" => this.ethnicity = insertMethod(this.ethnicity, param)
      case _ => noMatching(dest)
    }
  }
}
