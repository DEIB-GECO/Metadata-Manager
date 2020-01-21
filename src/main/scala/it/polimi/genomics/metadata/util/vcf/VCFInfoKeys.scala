package it.polimi.genomics.metadata.util.vcf

/**
 * Created by Tom on ott, 2019
 *
 */
object VCFInfoKeys {

  // keys
  val ANCESTRAL_ALLELE = "AA"
  val ALLELE_COUNT = "AC"
  val NUMBER_OF_ALLELES = "AN" // = NUMBER OF SAMPLES for haploid, * 2 if diploid, * 3 for triploid, and so on...
  val NUMBER_OF_SAMPLES = "NS"
  val ALLELE_FREQUENCY = "AF"
  val ALLELE_FREQUENCY_AFR = "AFR_AF"
  val ALLELE_FREQUENCY_AMR = "AMR_AF"
  val ALLELE_FREQUENCY_ASN = "ASN_AF"
  val CONFID_INTERVAL_AROUND_END_POS = "CIEND"
  val CONFID_INTERVAL_AROUND_POS = "CIPOS"
  val TOTAL_READ_DEPTH = "DP"
  val ALLELE_FREQUENCY_EAS = "EAS_AF"
  val SV_END = "END"
  val ALLELE_FREQUENCY_EUR = "EUR_AF"
  val VARIANT_IN_EXON_PULL_DOWN_BOUNDARIES = "EX_TARGET"
  val SV_IMPRECISE = "IMPRECISE"
  val MERGED_CALLS = "MC"
  val MOBILE_ELEM_INFO = "MEINFO"
  val MITOCHONDRIAL_INS_END = "MEND"
  val MITOCHONDRIAL_INS_LENGTH = "MLEN"
  val MITOCHONDRIAL_INS_START = "MSTART"
  val MULTI_ALLELIC_SITE = "MULTI_ALLELIC"
  val OLD_VARIANT_BEFORE_VT_NORM = "OLD_VARIANT"
  val ALLELE_FREQUENCY_SAN = "SAN_AF"   // seen only in extra_annotated version. Probably a typo for SAS
  val ALLELE_FREQUENCY_SAS = "SAS_AF"   // seen only in extra_annotated version. Probably a typo for EAS
  val SV_LENGTH = "SVLEN"
  val SV_TYPE = "SVTYPE"
  val PRECISE_TARGET_SITE_DUP = "TSD"
  val VARIANT_TYPE = "VT"

  // other
  val MOBILE_ELEM_INFO_PARTS_SEPARATOR = "|"

  val dictionary: Map[String, String] = Map(
    "AA"->"Ancestral Allele",
    "AC"->"Tot. Alternate Alleles",
    "AF"->"Estimated Allele Frequency")
  // TODO to be continued...

}
