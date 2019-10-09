package it.polimi.genomics.metadata.util.vcf

/**
 * Created by Tom on ott, 2019
 */
class VCFMutation(mutationLine: String) {
  import VCFMutation._
  private val mutationParts = mutationLine.split(COLUMN_SEPARATOR_REGEX).toList

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
  def info: VCFInfo ={
    new VCFInfo(
      mutationParts(7).split(INFO_SEPARATOR).map(singleInfo => {
      val eqIndex = singleInfo.indexOf("=")
      val key = singleInfo.substring(0, eqIndex)
      val value = singleInfo.substring(eqIndex+1)
      key -> value
    }).toMap)
  }
  /**
   * @param sampleName the sample name for which you want to read the attributes specified the FORMAT column of this mutation
   * @param biosamples the list of samples' names included included in this VCF file
   * @return an instance of {@link VCFFormat}
   * @throws IllegalArgumentException if the given argument sample name doesn't exist in the argument sample names
   * @throws Exception if the FORMAT keys string and the FORMAT values string have a different number of attributes as a
   *                   result of a wrong implementation of the VCF file standard.
   */
  def format(sampleName: String, biosamples: List[String]): VCFFormat = {
    val sampleNum = biosamples.indexOf(sampleName)
    if(sampleNum == -1)
      throw new IllegalArgumentException("ARGUMENT SAMPLE NAME IS NOT PART OF THE ARGUMENT SAMPLE NAME'S LIST")
    val formatFields = formatKeysAsString.split(FORMAT_SEPARATOR)
    val correspondingValues = formatValuesAsString(sampleNum).split(FORMAT_SEPARATOR)
    if(formatFields.length!=correspondingValues.length)
      throw new Exception("OOOUCH !! IT SEEMS WE HAVE A ROTTEN IMPLEMENTATION HERE!" +
        " I NICELY TRIED TO KILL YOUR PROCESS JUST TO LET YOU KNOW AND TAKE COUNTERMEASURES :)"+
        " THIS HAPPENED ON MUTATION "+mutationLine+" FOR SAMPLE "+sampleNum+". HAVE FUN.")
    new VCFFormat((formatFields zip correspondingValues).toMap)
  }

  private def formatKeysAsString:String ={
    mutationParts(8)
  }
  private def formatValuesAsString(sampleNum: Int):String ={
    mutationParts(9+sampleNum)
  }
}
object VCFMutation {
  val FORMAT_SEPARATOR = ":"
  val INFO_SEPARATOR = ";"
  val COLUMN_SEPARATOR_REGEX = "\\s+"
}
