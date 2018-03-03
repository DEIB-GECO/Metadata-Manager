package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.ExperimentType

class ExperimentTypeTCGA extends TCGATable with ExperimentType{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =
    dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => param.toUpperCase match{
      case "RNA-SEQ" => this.feature = "gene expression"
      case "DNA-SEQ" => this.feature = "mutations"
      case "MIRNA-SEQ" => this.feature =  "gene expression"
      case "METHYLATION ARRAY" => this.feature = "DNA-methylation"
      case "GENOTYPING ARRAY" => this.feature = "Copy Number Variation"
      case _ => insertMethod(this.feature, param)
    }
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case _ => noMatching(dest)
  }
}
