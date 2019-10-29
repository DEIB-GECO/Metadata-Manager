package it.polimi.genomics.metadata.util.vcf

import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}

/**
 * Created by Tom on ott, 2019
 */
object MetaInformation {

  var perGenotypeFields: Set[String] = Set.empty[String]
  var perAltFields: Set[String] = Set.empty[String]
  var perAlleleFields: Set[String] = Set.empty[String]
  var fixedCardinalityFields: Set[String] = Set.empty[String]

  def updatePropertiesFromMetaInformationLines(VCFFilePath: String):Unit ={
    perGenotypeFields = Set.empty[String]
    perAltFields = Set.empty[String]
    perAlleleFields = Set.empty[String]
    val fieldCardinalityPattern = PatternMatch.createPattern("##(FORMAT|INFO)=<ID=(\\w+),Number=(\\w+),.*")  //matches numbers, "A", "R", "G" but not "."
    val reader = FileUtil.open(VCFFilePath).get
    var aLine = reader.readLine()
    while(aLine.startsWith("##")){
      val formatParts = PatternMatch.matchParts(aLine, fieldCardinalityPattern)
      if(formatParts.nonEmpty)
        formatParts(2) match {
          case "A" => perAltFields += formatParts(1)
          case "G" => perGenotypeFields += formatParts(1)
          case "R" => perAlleleFields += formatParts(1)
          case _ =>
        }
      aLine = reader.readLine()
    }
    reader.close()
  }

}
