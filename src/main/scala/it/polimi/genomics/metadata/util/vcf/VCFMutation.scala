package it.polimi.genomics.metadata.util.vcf

import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by Tom on ott, 2019
 */
class VCFMutation(mutationLine: String, val headerMeta: HeaderMetaInformation) extends VCFMutationTrait {
  import VCFMutation._
  private val mutationParts = mutationLine.split(COLUMN_SEPARATOR_REGEX)
  lazy val info: Map[String, String] = _info
  private lazy val formatFields = formatKeysAsString.split(FORMAT_SEPARATOR)

  def chr:String ={
    mutationParts.head
  }
  def pos:String = {
    mutationParts(1)
  }
  def id:String = {
    mutationParts(2)
  }
  def ref:String ={
    mutationParts(3)
  }
  def alt:String ={
    mutationParts(4)
  }
  def qual:String ={
    mutationParts(5)
  }
  def filter:String ={
    mutationParts(6)
  }
  private def _info: Map[String, String] ={
//    try catch commented out to improve performance.
//    try {
      mutationParts(7).split(INFO_SEPARATOR).map(singleInfo => {
      val eqIndex = singleInfo.indexOf("=")
      if(eqIndex == -1)
        singleInfo -> "true"   // boolean flags may not have the assignment part
      else {
        val key = singleInfo.substring(0, eqIndex)
        val value = singleInfo.substring(eqIndex+1)
        key -> value
      }
    }).toMap
//    } catch {
//      case e: StringIndexOutOfBoundsException =>
//        println("EXCEPTION ON MUTATION "+pos+" INFO: "+mutationParts(7))
//        println(mutationParts(7).split(INFO_SEPARATOR).map(anInfo => "ERROR" -> anInfo).toMap)
//        Map.empty[String, String]
//    }
  }
  /**
   * @param sampleName the sample name for which you want to read the attributes specified the FORMAT column of this mutation
   * @param biosamples the list of samples' names included included in this VCF file
   * @return a Map of key -> value where the key is one of the FORMAT values, and the value is the corresponding value
   *         for this sample.
   * @throws IllegalArgumentException if the given argument sample name doesn't exist in the argument sample names
   * @throws Exception if the FORMAT keys string and the FORMAT values string have a different number of attributes as a
   *                   result of a wrong implementation of the VCF file standard.
   */
  def format(sampleName: String, biosamples: IndexedSeq[String]): Map[String, String] = {
    val sampleNum = biosamples.indexOf(sampleName)
    if(sampleNum == -1)
      throw new IllegalArgumentException("ARGUMENT SAMPLE NAME IS NOT PART OF THE ARGUMENT SAMPLE NAME'S LIST")
    val correspondingValues = formatValuesAsString(sampleNum).split(FORMAT_SEPARATOR)
    if(formatFields.length!=correspondingValues.length)
      throw new Exception("OOOUCH !! IT SEEMS WE HAVE A ROTTEN IMPLEMENTATION HERE!" +
        " I NICELY TRIED TO KILL YOUR PROCESS JUST TO LET YOU KNOW AND TAKE COUNTERMEASURES :)"+
        " THIS HAPPENED ON MUTATION "+mutationLine+" FOR SAMPLE "+sampleNum+". HAVE FUN.")
    (formatFields zip correspondingValues).toMap
  }

  private def formatKeysAsString:String ={
    mutationParts(8)
  }

  private def formatValuesAsString(sampleNum: Int):String ={
    mutationParts(9+sampleNum)
  }

}
object VCFMutation {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val FORMAT_SEPARATOR = ':'
  val INFO_SEPARATOR = ';'
  val INFO_MULTI_VALUE_SEPARATOR = ','
  val ALT_MULTI_VALUE_SEPARATOR = ','
  val ID_MULTI_VALUE_SEPARATOR = ';'
  val FORMAT_MULTI_VALUE_SEPARATOR = ','
  val COLUMN_SEPARATOR_REGEX = "\\s+"
  val MISSING_VALUE_CODE = "."

  def splitMultiValuedAlt(altString: String):Array[String]  ={
    altString.split(ALT_MULTI_VALUE_SEPARATOR)
  }

  def maxLengthAltAllele(altString: String): Long ={
    splitMultiValuedAlt(altString).map(_.length).max
  }

  def splitMultiValuedInfo(infoAttr: String):Array[String]  ={
    infoAttr.split(INFO_MULTI_VALUE_SEPARATOR)
  }

  /**
   * Splits variants occurring at the same locus in two or more VCFMutationTrait, one for each alternative allele in the
   * alt field of this mutation
   */
  def splitOnMultipleAlternativeMutations(mutation: VCFMutation): List[VCFMutationTrait] ={
    val numOfAlternatives = mutation.alt.count(_ == ALT_MULTI_VALUE_SEPARATOR)+1
    if(numOfAlternatives>1)
      Range(0, numOfAlternatives).toList.map(i => new VCFMultiAllelicSplitMutation(i, mutation))
    else
      List(mutation)
  }

  /**
   * Splits variants occurring at the same locus in two or more VCFMutationTrait, one for each alternative allele in the
   * alt field of this mutation
   */
  def splitOnMultipleAlternativeMutations(mutationLine: String, headerMeta: HeaderMetaInformation): List[VCFMutationTrait] ={
    val altField = mutationLine.split(COLUMN_SEPARATOR_REGEX)(4)
    val numOfAlternatives = altField.count(_ == ALT_MULTI_VALUE_SEPARATOR)+1
    if(numOfAlternatives>1)
      Range(0, numOfAlternatives).toList.map(i => new VCFMultiAllelicSplitMutation(i, new VCFMutation(mutationLine, headerMeta)))
    else
      List(new VCFMutation(mutationLine, headerMeta))
  }

  ////////////////////////////////////////////    EXTENSION OF MAP ATTRIBUTES   //////////////////////////////////////

  implicit class MutationProperties(map: Map[String, String]) {

    def genotype: Option[String] = {
      map.get(VCFFormatKeys.GENOTYPE)
    }

    def isMutated: Boolean = {
      genotype match {
        case Some(gt) => gt.contains("1")
        case None => false
      }
    }

    def ploidy: Int ={
      map.get(VCFFormatKeys.GENOTYPE) match {
        case Some(gt) => gt.count(_ == '/')+gt.count(_=='|')+1
        case None => 0
      }
    }
  }

}

