package it.polimi.genomics.metadata.util.vcf

/**
 * Created by Tom on ott, 2019
 *
 */
class VCFInfo(infoKeyValueMap: Map[String, String]) {

  def getMap:Map[String, String] ={
    infoKeyValueMap
  }

  def get(infoKey: String): Option[String] ={
    infoKeyValueMap.get(infoKey)
  }
}
object VCFInfo {

  val ANCESTRAL_ALLELE = "AA"
  val ALTERNATE_ALLELE_COUNT = "AC"
  val ALLELE_FREQUENCY = "AF"
  val ALLELE_FREQUENCY_AFR = "AFR_AF"
  val ALLELE_FREQUENCY_AMR = "AMR_AF"
  val ALLELE_FREQUENCY_ASN = "ASN_AF"
  val CONFID_INTERVAL_AROUND_END_POS = "CIEND"
  val CONFID_INTERVAL_AROUND_POS = "CIPOS"
  val TOTAL_READ_DEPTH = "DP"
  val ALLELE_FREQUENCY_EAS = "EAS_AF"
  val END_POS = "END"
  val ALLELE_FREQUENCY_EUR = "EUR_AF"
  val VARIANT_IN_EXON_PULL_DOWN_BOUNDARIES = "EX_TARGET"
  val SV_IMPRECISE = "IMPRECISE"
  val MERGED_CALLS = "MC"
  val MOBILE_ELEM_INFO = "MEINFO"
  val END_POS_MITOCHONDRIAL_INS = "MEND"
  val LENGTH_MITOCHONDRIAL_INS = "MLEN"
  val START_POS_MITOCHONDRIAL_INS = "MSTART"
  val MULTI_ALLELIC_SITE = "MULTI_ALLELIC"
  val OLD_VARIANT_BEFORE_VT_NORM = "OLD_VARIANT"
  val ALLELE_FREQUENCY_SAN = "SAN_AF"
  val ALLELE_FREQUENCY_SAS = "SAS_AF"
  val SV_LENGTH = "SVLEN"
  val SV_TYPE = "SVTYPE"
  val PRECISE_TARGET_SITE_DUP = "TSD"
  val VARIANT_TYPE = "VT"


  val dictionary: Map[String, String] = Map(
    "AA"->"Ancestral Allele",
    "AC"->"Tot. Alternate Alleles",
    "AF"->"Estimated Allele Frequency")
  // TODO to be continued...

}
