package it.polimi.genomics.importer.ModelDatabase.Utils

import it.polimi.genomics.importer.RemoteDatabase.DbHandler

object InsertMethod {

  def selectInsertionMethod(sourceKey: String, globalKey: String, method: String) = (actualParam: String, newParam: String) => {
    method.toUpperCase() match {
      case "MANUALLY" => if(sourceKey == "null") null else sourceKey
      case "CONCAT-MANUALLY" => if(actualParam == null) sourceKey else actualParam.concat(" " + sourceKey)
      case "CONCAT" => if (actualParam == null) newParam else actualParam.concat(" " + newParam)
      case "CONCAT-FINAL" => if (actualParam == null) newParam else actualParam.concat(" " + newParam)
      case "CONCAT-NOSPACE" => if (actualParam == null) newParam else actualParam.concat(newParam)
      case "CHECK-PREC" => if(actualParam == null) newParam else actualParam
      case "SELECT-CASE-TCGA" => if (actualParam == null) DbHandler.getSourceSiteByCode(newParam)else actualParam.concat(" " + DbHandler.getSourceSiteByCode(newParam))
      case "DEFAULT" => newParam
      case "SELECT-FEATURE-TCGA" => {
        if(actualParam == null)
        newParam match{
          case "RNA-seq" => "gene expression"
          case "DNA-seq" =>  "mutations"
          case "miRNA-Seq" => "gene expression"
          case "Methylation Array" => "DNA-methylation"
          case "Genotyping Array" => "Copy Number Variation"
        }
        else
          actualParam.concat(" " + newParam match{
            case "RNA-seq" => "gene expression"
            case "DNA-seq" =>  "mutations"
            case "miRNA-Seq" => "gene expression"
            case "Methylation Array" => "DNA-methylation"
            case "Genotyping Array" => "Copy Number Variation"
          })
      }
      case _ => actualParam
    }
  }
}
