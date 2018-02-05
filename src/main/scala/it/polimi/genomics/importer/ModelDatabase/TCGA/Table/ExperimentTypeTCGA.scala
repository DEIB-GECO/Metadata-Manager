package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.ExperimentType

class ExperimentTypeTCGA extends TCGATable with ExperimentType{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =
    dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => this.feature = insertMethod(this.feature, param)/*param match{
      case "RNA-seq" => this.feature = "gene expression"
      case "DNA-seq" => this.feature = "mutations"
      case "miRNA-Seq" => this.feature =  "gene expression"
      case "Methylation Array" => this.feature = "DNA-methylation"
      case "Genotyping Array" => this.feature = "Copy Number Variation"
    }*/
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case _ => noMatching(dest)
  }
}
