package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.ExperimentType

class ExperimentTypeTCGA extends TCGATable with ExperimentType{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =
    dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => param.toUpperCase match{
      case "RNA-SEQ" => this.feature = "gene expression" //gene expression quantification
      case "MIRNA-SEQ" => this.feature =  "gene expression" //isoform expression quantification, mirna expression quantification
      case "DNA-SEQ" => this.feature = "mutations" //masked somatic mutations
      case "METHYLATION ARRAY" => this.feature = "DNA-methylation" //methylation beta value
      case "GENOTYPING ARRAY" => this.feature = "copy number variation" //copy number segment, masked copy number segment
      case _ => insertMethod(this.feature, param)
    }
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case _ => noMatching(dest)
  }
}
