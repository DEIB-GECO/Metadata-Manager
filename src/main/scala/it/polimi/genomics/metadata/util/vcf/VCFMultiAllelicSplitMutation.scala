package it.polimi.genomics.metadata.util.vcf

import it.polimi.genomics.metadata.util.vcf.VCFMutation.{ALT_MULTI_VALUE_SEPARATOR, ID_MULTI_VALUE_SEPARATOR, INFO_MULTI_VALUE_SEPARATOR, FORMAT_MULTI_VALUE_SEPARATOR}

/**
 * Created by Tom on ott, 2019
 */
class VCFMultiAllelicSplitMutation(alternativeNum: Int, m: VCFMutation) extends VCFMutationTrait {

  val multiAllelic = true

  def chr:String ={
    m.chr
  }

  def pos:String ={
    m.pos
  }

  def id:String ={
    val splitIds = m.id.split(ID_MULTI_VALUE_SEPARATOR)
    if(alternativeNum < splitIds.length) splitIds(alternativeNum) else m.id
  }

  def ref:String ={
    m.ref
  }

  def alt:String ={
    val splitAlternatives = m.alt.split(ALT_MULTI_VALUE_SEPARATOR)
    if(alternativeNum < splitAlternatives.length) splitAlternatives(alternativeNum) else m.alt
  }

  def qual:String ={
    m.qual
  }

  def filter:String = {
    m.filter
  }

  lazy val info:Map[String, String] ={
    m.info.map( kvpair => {
      (kvpair._1, {
        val splitValues = kvpair._2.split(INFO_MULTI_VALUE_SEPARATOR)
        // if nothing to split, just return the value
        if(splitValues.length == 1 && splitValues.head == VCFMutation.MISSING_VALUE_CODE)
          kvpair._2
        // VCF field with cardinality A
        else if (m.headerMeta.perAltFields.contains(kvpair._1))
          splitValues(alternativeNum)
        // VCF field with cardinality R
        else if (m.headerMeta.perAlleleFields.contains(kvpair._1)) {
          splitValues(0) + INFO_MULTI_VALUE_SEPARATOR + splitValues(alternativeNum)
        } else
          kvpair._2
      })
    })
  }

  def format(sampleName: String, biosamples: IndexedSeq[String]): Map[String, String] = {
//    try {
      val formatMap = m.format(sampleName, biosamples)
      formatMap.map(kvpair => {
        (kvpair._1, {
          val splitValues = kvpair._2.split(FORMAT_MULTI_VALUE_SEPARATOR)
          // if nothing to split, just return the value
          if(splitValues.length == 1 && splitValues.head == VCFMutation.MISSING_VALUE_CODE)
            kvpair._2
          // VCF field with cardinality A
          else if (m.headerMeta.perAltFields.contains(kvpair._1))
            splitValues(alternativeNum)
          // VCF field with cardinality R
          else if (m.headerMeta.perAlleleFields.contains(kvpair._1)) {
            splitValues(0) + FORMAT_MULTI_VALUE_SEPARATOR + splitValues(alternativeNum)
          }
          // VCF field with cardinality G
          else if (m.headerMeta.perGenotypeFields.contains(kvpair._1)) {
            import it.polimi.genomics.metadata.util.vcf.VCFMutation.MutationProperties
            import VCFMultiAllelicSplitMutation._
            val ploidy = formatMap.ploidy
            /*
            * In case of haploid calls, attributes with cardinality "G" (genotype) have one value for each possible
            * genotype (allele) described in alt and ref. The value at index 0 relates to the ref genotype,
            * while the values for alt genotypes have indices 1, 2, 3,..., N.
            */
            if (ploidy == 1)
              splitValues(alternativeNum + 1)
            /*
             * For diploid calls, genotypes are given from the combination of all the possible genotypes, one pair at a
             * time. The index of each pair is given by the function "veryComplicatedIndex"
             */
            else if (ploidy == 2) {
              (0 to countAlternativeAlleles).map(allele => splitValues(veryComplicatedIndex(alternativeNum, allele))).mkString(FORMAT_MULTI_VALUE_SEPARATOR.toString)
            }
            // for triploid calls,  it's not clear from the documentation how the index should be computed
            else
              kvpair._2
          }
          // VCF fields with cardinality unbounded (".") or fixed (a number)
          else
            kvpair._2
        })
      })
//    } catch {
//      case e: Exception => println("exception of muation: "+String.join(" ",
//        m.chr, m.pos, m.id, m.ref, m.alt, m.info.toString()
//      ))
//        println(m.format(sampleName, biosamples).toString())
//        val formatValuesAsString = m.formatValuesAsString(biosamples.indexOf(sampleName))
//        println(formatValuesAsString.split(":").mkString("\n"))
//        throw e
//    }
  }

  def isSampleMutated(formatOfSample: Map[String, String]): Boolean ={
    val gt = formatOfSample.get(VCFFormatKeys.GENOTYPE)
    gt.isDefined && gt.get.contains((alternativeNum+1).toString)
  }

  def mutatedChromosomeCopy(formatOfSample: Map[String, String]): List[String] ={
    val gt = formatOfSample.get(VCFFormatKeys.GENOTYPE)
    if(gt.isEmpty)
      List(VCFMutation.MISSING_VALUE_CODE)
    else {
      val condensedGT = gt.get.replaceAll("\\|", "").replaceAll("/", "")
      val numberOfAlleleAsChar = (48+alternativeNum+1).toChar  //48 is for character encoding and 1 is to convert alternativeNum in 1-based notation
      condensedGT.toList.map(num => if(num == numberOfAlleleAsChar) "1" else "0")
    }
  }

  private def countAlternativeAlleles:Int ={
    m.alt.split(ALT_MULTI_VALUE_SEPARATOR).length
  }

}
object VCFMultiAllelicSplitMutation {
  /**
   * As for VCF specifications version 4.3, fields with Genotype cardinality (Those attributes are described within
   * the meta-information lines with ##FORMAT< ID=..., Number=G, ...) have a list of values, one for each possible genotype
   * (i.e. pair of alleles belonging either to the ref or alt group).
   * For haploid calls, the value related to the i-th alternative is just the i-th value.
   * For diploid calls, the genotypes combinations are 3 to the power of the number alleles (considering ref and alt);
   * for example if A is the ref allele, and B is the only alt allele, the possible combinations are: AA,AB,BB
   * listed in this order; if A is the ref allele and the alt alleles are B and C, the possible combinations are:
   * AA,AB,BB,AC,BC,CC.
   * From the specification, it can be read: if diploid, the index of the pair of alleles (a,b), where a <=b, is b*(b+1)/2 + a
   *
   * @param IdxAllele1 can be the index of ref (0) or the index of an alt allele (1, 2, ...), possibly equal to IdxAllele2
   * @param IdxAllele2 can be the index of ref (0) or the index of an alt allele (1, 2, ...), possibly equal to IdxAllele1
   * @return the index of the value of a filed with cardinality G (genotype). That value relates the alleles whose
   *         indices have been given as argument to this function.
   */
  private def veryComplicatedIndex(IdxAllele1:Int, IdxAllele2:Int):Int ={
    val a = Math.min(IdxAllele1, IdxAllele2)
    val b = Math.max(IdxAllele1, IdxAllele2)
    b*(b+1)/2 + a
  }

}
