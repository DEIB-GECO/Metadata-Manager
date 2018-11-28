package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.ExperimentType

class ExperimentTypeTCGA extends TCGATable with ExperimentType{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit =
    dest.toUpperCase()  match{
    case "TECHNIQUE" => param match{
      case "cnv" => this.technique = "snp array"
      case "mirnaseq" => this.technique = "mirna-seq"
      case "rnaseq" => this.technique = "rna-seq"
      case "rnaseqv2" => this.technique = "rna-seq"
      case "dnaseq" => this.technique = "dna-seq"
      case "dnamethylation450" => this.technique = "dna methylation 450"
      case "dnamethylation27" => this.technique = "dna methylation 27"
      case _ => this.technique = insertMethod(this.technique, param)
    }
    case "FEATURE" => param match{
      case "Transcriptome Profiling" => this.feature = "gene expression" //new opengdc: gene expression quantification
      case "cnv" => this.feature = "copy number variation" //old tcga:cnv
      case "rnaseq"|"mirnaseq" => this.feature = "gene expression" //old tcga:mirnaseq..rnaseq..,rnaseqv2..
      case "dnaseq" => this.feature = "mutations" //old tcga:dnaseq
      case "dnamethylation450"|"dnamethylation27" => this.feature = "DNA Methylation" //old tcga:dnamethylation
      case _ => this.feature = insertMethod(this.feature, param)
    }
   /* case "FEATURE" => param.toUpperCase match{
      case "RNA-SEQ" => this.feature = "gene expression" //gene expression quantification
      case "MIRNA-SEQ" => this.feature =  "gene expression" //isoform expression quantification, mirna expression quantification
      case "DNA-SEQ" => this.feature = "mutations" //masked somatic mutations
      case "METHYLATION ARRAY" => this.feature = "DNA-methylation" //methylation beta value
      case "GENOTYPING ARRAY" => this.feature = "copy number variation" //copy number segment, masked copy number segment
      case _ => insertMethod(this.feature, param)
    }*/
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case _ => noMatching(dest)
  }
}
