package it.polimi.genomics.metadata.util.vcf

import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}

/**
 * Object containing meta-information about a single VCF file, like the type and cardinality of the attributes
 * declared in the Meta-Information section (the on preceding the header line) of the file.
 *
 * Created by Tom on ott, 2019
 */
class HeaderMetaInformation(VCFFilePath: String) {

  // cardinality sets
  /** Set of fields with genotype cardinality: for haploid calls the attributes in this set have one value per allele
   * (both for reference and alternative set), while for diploid calls they've one value for each possible pair of
   * alleles (again considering both reference and alternatives) */
  var perGenotypeFields: Set[String] = Set.empty[String]
  /** Set of fields with same cardinality as the number of alternative alleles */
  var perAltFields: Set[String] = Set.empty[String]
  /** Set of fields with same cardinality as the sum alternative alleles number + reference allele number (the latter always == 1)  */
  var perAlleleFields: Set[String] = Set.empty[String]
  /** Set of fields used as flags. If the field is present, it is true, otherwise false */
  var flagFields:Set[String] = Set.empty[String]
//  var fixedCardinalityFields: Set[String] = Set.empty[String]     // currently unused

  // category sets  // currently unused
//  /** Set of fields used in the INFO column of a VCF mutation */
//  var infoFields: Set[String] = Set.empty[String]
//  /** Set of fields used in the FORMAT column of a VCF mutation */
//  var formatFields: Set[String] = Set.empty[String]

  {
    val fieldCardinalityPattern = PatternMatch.createPattern("##FORMAT=<ID=(\\w+),Number=(\\.|\\w+),.*")  //matches numbers, strings and "."
    val infoFieldCardinalityPattern = PatternMatch.createPattern("##INFO=<ID=(\\w+),Number=(\\.|\\w+),.*")  //matches numbers, strings and "."
    val reader = FileUtil.open(VCFFilePath).get
    var aLine = reader.readLine()
    while(aLine.startsWith("##")){
      val formatParts = PatternMatch.matchParts(aLine, fieldCardinalityPattern)
      val infoParts = PatternMatch.matchParts(aLine, infoFieldCardinalityPattern)
      if(formatParts.nonEmpty) {
        categorizeByCardinality(formatParts.head, formatParts(1))
//        formatFields += formatParts.head      // currently unused
      } else if(infoParts.nonEmpty) {
        categorizeByCardinality(infoParts.head, infoParts(1))
//        infoFields += infoParts.head          // currently unused
      }
      aLine = reader.readLine()
    }
    reader.close()
  }

  private def categorizeByCardinality(fieldName:String, fieldCardinalityDescriptor:String):Unit ={
    fieldCardinalityDescriptor match {
      case "A" => perAltFields += fieldName
      case "G" => perGenotypeFields += fieldName
      case "R" => perAlleleFields += fieldName
      case "0" => flagFields += fieldName
      case _ =>
    }
  }


}
