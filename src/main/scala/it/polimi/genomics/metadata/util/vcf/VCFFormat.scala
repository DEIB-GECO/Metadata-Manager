package it.polimi.genomics.metadata.util.vcf

/**
 * Created by Tom on ott, 2019
 */
class VCFFormat(formatKeyValuesMap: Map[String, String]) {
  import VCFFormat._

  def hasMutation:Boolean ={
    genotype match {
      case Some(gt) => gt.contains("1")
      case None => false
    }
  }

  def genotype: Option[String] = {
    formatKeyValuesMap.get(GENOTYPE)
  }

  def getMap:Map[String, String] ={
    formatKeyValuesMap
  }

  def get(formatKey: String): Option[String] ={
    formatKeyValuesMap.get(formatKey)
  }

}
object VCFFormat {
  val GENOTYPE = "GT"
  val COPY_NUM_GT_IMPRECISE_EV = "CN"
  val COPY_NUM_LIKELIHOODS_NO_FREQ_PRIOR = "CNL"
  val COPY_NUM_LIKELIHOODS = "CNQ"
  val GT_LIKELIHOOD = "GP"
  val GENOTYPE_QUALITY = "GQ"
  val GT_FILTER_PER_SAMPLE = "FT"
  val GT_LIKELIHOODS_PHRED = "PL"
  val READ_DEPTH_PER_SAMPLE = "DP"

  val dictionary: Map[String, String] = Map(
    GENOTYPE->"Genotype")
  // TODO to be continued...

  /*  val GT_PHASED_NOT_MUTATED_REGEX = PatternMatch.createPattern("(\\d)/(\\d).*|(\\d)\\|(\\d).*|(\\d).*")
    val GT_NOT_MUTATED = "0"*/
}
