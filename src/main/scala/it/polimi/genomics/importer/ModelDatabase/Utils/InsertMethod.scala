package it.polimi.genomics.importer.ModelDatabase.Utils

import it.polimi.genomics.importer.RemoteDatabase.DbHandler

object InsertMethod {

  def selectInsertionMethod(sourceKey: String, globalKey: String, method: String, concatCharacter: String, subCharacter: String, newCharacter: String, remCharacter: String) = (actualParam: String, newParam: String) => {
    method.toUpperCase() match {
      case "MANUALLY" => if(sourceKey == "null") null else sourceKey
      case "CONCAT-MANUALLY" => if(actualParam == null) sourceKey else actualParam.concat(concatCharacter + sourceKey)
      case "CONCAT" => if (actualParam == null) newParam else actualParam.concat(concatCharacter + newParam)
      case "CONCAT-NOSPACE" => if (actualParam == null) newParam else actualParam.concat(newParam)
      case "CHECK-PREC" => if(actualParam == null) newParam else actualParam
      case "SELECT-CASE-TCGA" => if (actualParam == null) DbHandler.getSourceSiteByCode(newParam)else actualParam.concat(concatCharacter + DbHandler.getSourceSiteByCode(newParam))
      case "DEFAULT" => newParam
      case "SUB-CONCAT" => this.replaceAndConcat(actualParam, newParam, subCharacter, newCharacter, concatCharacter)
      case "SUB-REMOVE-CONCAT" => this.replaceAndConcat(actualParam, this.substituteWordWith(newParam, remCharacter, ""), subCharacter, newCharacter, concatCharacter)
      case "REMOVE-CONCAT" => if (actualParam == null) this.substituteWordWith(newParam, remCharacter, "") else actualParam.concat(concatCharacter + this.substituteWordWith(newParam, remCharacter, ""))
      /*case "SELECT-FEATURE-TCGA" => {
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
      }*/
      case _ => actualParam
    }
  }

  def substituteWordWith(phrase: String, wordToSub: String, wordToIns: String): String = {
      phrase.replace(wordToSub,wordToIns)
  }

  def replaceAndConcat(actualParam: String, newParam: String, wordToSub: String, wordToIns: String, division: String): String  =  {
      if (actualParam == null) this.substituteWordWith(newParam, wordToSub, wordToIns) else actualParam.concat(division + this.substituteWordWith(newParam, wordToSub, wordToIns))
  }


}
