package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.BufferedReader

import it.polimi.genomics.metadata.util.vcf.VCFMutation.{ALT_MULTI_VALUE_SEPARATOR, MutationProperties, maxLengthAltAllele, splitMultiValuedInfo}
import it.polimi.genomics.metadata.util.vcf.{VCFInfoKeys, VCFMutation}
import it.polimi.genomics.metadata.util.{FileUtil, RoughReadProgress}
import org.slf4j.{Logger, LoggerFactory}
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.VCFAdapter.OKGMutation

import scala.util.{Failure, Success}

/**
 * Created by Tom on ott, 2019
 *
 * This class performs data content extraction for 1000Genomes' specific Variant Call Format files. As such it makes
 * some assumptions on the kind and format of information available.
 *
 * **Notes: VCF files from 1kGenomes don't report the strand in which the mutation has been detected, however the source
 * states "All the variants in both our VCF files and on the browser are always reported on the forward strand."
 * (source: https://www.internationalgenome.org/faq/what-strand-are-variants-your-vcf-file/ last time checked on 9 Oct 19).
 */
class VCFAdapter(VCFFilePath: String) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val biosamples: List[String] = biosamples(VCFFilePath)
  private var numberOfLinesInFile: Option[Long] = None
  val MISSING_VALUE_CODE = "*"

  ////////////////////////////////////  SAMPLE -> VARIANTS   ///////////////////////////////////////////////////////////
  /**
   * Given a sample name belonging to this VCF file, this method adds to the target file the region attributes of the
   * mutations found in the sample.
   *
   * @param sampleName the sample whose mutations have to be included in the file.
   * @param toFilePath the output file where to write the mutations.
   * @return false if the sample doesn't appear in this VCf, true otherwise.
   */
  def appendMutationsOf(sampleName: String, toFilePath: String): Boolean = {
    val index = biosamples.indexOf(sampleName)
    if(index == -1)
      false // the sample isn't listed in this VCF
    else {
      val writer = FileUtil.writeAppend(toFilePath).get  // let it throw exception if failed
      var targetFileEmpty = FileUtil.size(toFilePath) == 0
      val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
      advanceAndGetHeaderLine(reader)   // advance reader and skip header line
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read each mutation
        val mutation = new VCFMutation(mutationLine)
        val formatOfSample = mutation.format(sampleName, biosamples)
        if(formatOfSample.isMutated){
          val infoFields = mutation.info
          // write it if own by the sample
          val line = OneKGTransformer.tabber(
            mutation.chr,
            mutation.pos,
            mutation.end.toString,
            "+",   // strand (see class documentation for explanations)
            mutation.id,
            mutation.ref,
            mutation.alt,
            infoFields.getOrElse(VCFInfoKeys.ALLELE_FREQUENCY, MISSING_VALUE_CODE)
            //TODO go on with other attributes based on the desired schema for region data
          )
          if(!targetFileEmpty)
            writer.newLine()
          else
            targetFileEmpty = false
          writer.write(line)
        }
      }, setupReadProgressCanary(VCFFilePath))
      writer.close()
      true
    }
  }

  ////////////////////////////////////  VARIANT -> SAMPLES   ///////////////////////////////////////////////////////////

  def getSamplesWithMutation(mutationLine: String): List[String] ={
    val mutation = new VCFMutation(mutationLine)
    biosamples.filter(sample => {
      mutation.format(sample, biosamples).isMutated
    })
  }

  ////////////////////////////////////  META INFORMATION & HEADER LINES    ///////////////////////////////////////////////

  def advanceAndGetHeaderLine(reader: BufferedReader): String ={
    var headerLine = reader.readLine()
    while(headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    headerLine
  }

  def advanceAndGetFirstMutation(reader: BufferedReader): String ={
    var firstVariantLine = reader.readLine()
    while(firstVariantLine.startsWith("#")) {
      firstVariantLine = reader.readLine()
    }
    firstVariantLine
  }

  /////////////////////////////////////   HELPER METHODS    ///////////////////////////////////////////////////////////

  /**
   * @param VCFFilePath the path (relative to the project's root dir) to a VCF file complete of genotype information.
   * @return an array of biosamples' names read from the heaser line of the argument VCF file given
   */
  private def biosamples(VCFFilePath: String): List[String] = {
    val reader = FileUtil.open(VCFFilePath).get
    val headerLine = advanceAndGetHeaderLine(reader)
    reader.close()
    // skip mandatory header columns
    val samplesString = headerLine.split("FORMAT", 2)(1).trim
    // create array of biosamples' names
    samplesString.split(VCFMutation.COLUMN_SEPARATOR_REGEX).toList
  }

  /**
   * Prepares an instance of RoughReadProgress to receive progress updates while processing files very large in size.
   * Progress are not logged through a logger instance as they're useful only for a user who wants to have an estimate
   * of the time left to finish.
   * This estimate is just a rough approximation and requires some more seconds before the file processing can begin in
   * order to compute the necessary parameters.
   */
  private def setupReadProgressCanary(fullFilePath: String): Option[RoughReadProgress] ={
    if(numberOfLinesInFile.isDefined)
      Some(new RoughReadProgress(numberOfLinesInFile.get, 10, RoughReadProgress.notifyProgress))
    else {
      println("COUNTING LINES OF FILE: " + fullFilePath)
      FileUtil.countLines(fullFilePath) match {
        case Failure(_) =>
          println("COUNT OF LINES IN FILE FAILED")
          None
        case Success(value) =>
          numberOfLinesInFile = Some(value)
          Some(new RoughReadProgress(value, 10, RoughReadProgress.notifyProgress))
      }
    }
  }

  /** TEST METHODS  */

  /**
   * ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf mutations have all and only VARIANT_TYPE attr with value SNP | INDEL
   * ALL.chr22.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf mutations have (all up to Excel's limits) and only VARIANT_TYPE attr with value SNP | INDEL
   *
   * ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf mutations have all only VARIANT_TYPE attr with value S (snp) | I (indel) | M (mnp)
   * ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf have
   *        all VARIANT_TYPE attr with values SNP | INDEL | MNP | SV
   *        when SV_END is defined, VARIANT_TYPE=SV and SV_TYPE (e.g. cnv) is defined
   * ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf has
   *        all VARIANT_TYPE attr with values SNP | INDEL | MNP | SV | SNP,INDEL (a mutation resulting in either an SNP or INDEL at the same site)
   *        when SV_END is defined, VARIANT_TYPE=SV and SV_TYPE (e.g. cnv|del|dup) is defined
   *        when MOBILE_ELEM_INFO is defined (eg: ALU|CNV|DEL|DUP), VARIANT_TYPE=SV and SV_END is defined when ALT is copy number variant or a single base but not when ALT is an insertion mobile element event
   * ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf has
   *        all VARIANT_TYPE attr with values SNP | INDEL | SV | SNP,INDEL
   *        when SV_LENGTH is defined, VARIANT_TYPE=SV, SV_TYPE (e.g. alu|sva) is defined, MOBILE_ELEM_INFO is defined (e.g. SVA,315,1627,+|AluUndef,1,177,-) and ALT is an insertion mobile element event
   *        when SV_LENGTH is not defined, if(VARIANT_TYPE=SV) SV_TYPE is CNV|DEL|DUP, SV_END is defined and ALT is a copy number variation (e.g. <cn0>).
   *        when SV_END is defined, VARIANT_TYPE=SV and SV_TYPE (e.g. cnv|del|dup) is defined
   * ALL.chr22.phase3_shapeit2_mvncall_integrated_v5_extra_anno.20130502.genotypes.vcf has
   *        no VARIANT_TYPE; the presence of SV_TYPE denotes a structural variant, otherwise it could be snp, indel or mnp
   *        when SVLEN is defined when MEINFO is defined and SV_TYPE reports the ME type, but also when SV has SV_TYPE DEL|DUP|INV
   *        when SV_TYPE is defined and MEINFO is not, SVEND is defined
   *        when SVLEN is defined, SV_TYPE is defined (ALU|DEL|DUP|INV|LINE1|SVA)
   *
   */
  def TESTForLengthAvailableInformation(inputFile: String, outputFile: String): Unit = {
    import VCFInfoKeys._
    import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.VCFAdapter.OKGMutation
    val writer = FileUtil.writeAppend(outputFile).get
    var targetFileEmpty = FileUtil.size(outputFile) == 0
    val reader = FileUtil.open(inputFile).get
    advanceAndGetHeaderLine(reader)
    FileUtil.scanFileAndClose(reader, mutationLine => {
      val m = new VCFMutation(mutationLine)
      var missingStuff: String = ""
      val infoString = List(
        m.info.getOrElse(VARIANT_TYPE, {
          missingStuff = OneKGTransformer.tabberConcat(missingStuff.concat("MISSING "+VARIANT_TYPE))
          MISSING_VALUE_CODE}),
        m.info.getOrElse(SV_LENGTH, MISSING_VALUE_CODE),
        m.info.getOrElse(SV_END, MISSING_VALUE_CODE),
        m.info.getOrElse(SV_TYPE, MISSING_VALUE_CODE),
        m.info.getOrElse(MOBILE_ELEM_INFO, MISSING_VALUE_CODE),
        m.info.getOrElse(MITOCHONDRIAL_INS_LENGTH, MISSING_VALUE_CODE),
        m.info.getOrElse(MITOCHONDRIAL_INS_START, MISSING_VALUE_CODE),
        m.info.getOrElse(MITOCHONDRIAL_INS_END, MISSING_VALUE_CODE)
      )
      val line = OneKGTransformer.tabber(m.chr, m.pos, m.end.toString, m.ref, m.alt)+OneKGTransformer.tabberConcat(infoString)+OneKGTransformer.tabberConcat(missingStuff)
      writer.write(line)
      if(!targetFileEmpty)
        writer.newLine()
      else
        targetFileEmpty = false
    }, setupReadProgressCanary(VCFFilePath))
    writer.close()
  }


  def TESTaccessFields(inputFile: String): Unit = {
    val reader = FileUtil.open(inputFile).get
    advanceAndGetHeaderLine(reader)
    val aVariant = reader.readLine()
    reader.close()
    println(aVariant)
    val mutation = new VCFMutation(aVariant)
    println("chr "+mutation.chr)
    println("pos "+mutation.pos)
    println("id "+mutation.id)
    println("ref "+mutation.ref)
    println("alt "+mutation.alt)
    println("qual "+mutation.qual)
    println("filter "+mutation.filter)
    println("info "+mutation.info)

    var format = mutation.format("HG01770", biosamples)
    println("HG01770 has genotype "+format.genotype+". Is it mutated ? "+format.isMutated)

    format = mutation.format("HG01124", biosamples)
    println("HG01124 has genotype "+format.genotype+". Is it mutated ?"+format.isMutated)
  }

}
object VCFAdapter {

  /**
   * This class extends the general VCFMutation with methods and attributes whose implementation is specific for 1000Genomes,
   * since they are not mandatory VCF attributes and so their definition depends on the custom attributes specified in the file
   * by each source.
   *
   * Example cases are mutation's end and length.
   */
  implicit class OKGMutation(m: VCFMutation){

    lazy val length: Long = _length

    /**
     * @return End position of this mutation.
     *         For SNPs, INDELs or MNP of nuclear and mitochondrial DNA, it's computed from the characteristics of the ALT alleles.
     *         For mobile element SVs, it relies on the presence of the INFO attribute "SVLEN" or "MEINFO" with
     *         well defined START and END sub-properties.
     *         For all the other SVs, it uses "SVLEN" if present or "END" INFO attributes.
     *         If none of the above cases is positive and SVTYPE is "ALU", then an approximate length of 300 bases is returned.
     *         If nothing of the above strategies works, end has the same value of the starting pos.
     */
    def end:Long ={
      length+m.pos.toLong
    }

    /**
     * Since length computation can be expensive, it's saved on first access.
     * For details about length computation see method "end" of implicit class OKGMutation
     */
    private def _length:Long ={
      import VCFInfoKeys._
        // INDELs
        if (m.info.get(VARIANT_TYPE).isDefined && m.info(VARIANT_TYPE).contains("I") /*cover also the case VT="INDEL"*/ ||
          m.info.get(SV_TYPE).isEmpty && m.ref.length != maxLengthAltAllele(m.alt))
        // In INDELS, ALT and m.ref share the first or the last base. Moreover, ALT can have more than one ALT sequence comma separated
          Math.max(m.ref.length, m.alt.split(ALT_MULTI_VALUE_SEPARATOR).map(_.length).max) - 1
        // MNP
        else if (m.info.get(VARIANT_TYPE).isDefined && m.info(VARIANT_TYPE).contains("M") /*cover also the case VT="MNP"*/ ||
          m.info.get(SV_TYPE).isEmpty && m.ref.length == maxLengthAltAllele(m.alt))
        // MNP mutations 're like concatenated SNPs and they don't share the first or the last base.
          m.ref.length
        // SNP
        else if (m.info.get(SV_TYPE).isEmpty  && /* without this condition it would accept also SVs having VARIANT_TYPE="SV" */
          ( m.info.get(VARIANT_TYPE).isDefined && m.info(VARIANT_TYPE).contains("S") /*cover also the case VT="SNP"*/ ||
            m.ref.length == 1 && maxLengthAltAllele(m.alt) == 1) )
          1
        // SVs with SV_LENGTH like mobile elements and some others (DEL|DUP|INV)
        else if (m.info.get(SV_LENGTH).isDefined)
        // max of absolute values of SV_LENGTH. SV_LENGTH is negative for SVs representing long deletions
          splitMultiValuedInfo(m.info(SV_LENGTH)).map(_.toLong.abs).max
        //  all SVs which are not mobile elements should have this attribute: CNV|DEL|DUP|INV|INS
        else if (m.info.get(SV_END).isDefined)
          m.info(SV_END).toLong - m.pos.toLong
        // fallback case for mobile elements without SV_LENGTH (never observed case but... you know, just to be sure)
        else if (m.info.get(MOBILE_ELEM_INFO).isDefined) {
          val parts = m.info(MOBILE_ELEM_INFO).split(MOBILE_ELEM_INFO_PARTS_SEPARATOR)
          try {
            parts(2).toLong - parts(1).toLong
          } catch {
            case _: Exception =>
                VCFMutation.logger.error("MEINFO unexpected format in mutation "+mutationStringWithoutFormat)
                0
          }
        } else if(m.info.get(SV_TYPE).isDefined && m.info(SV_TYPE)=="ALU") {  // peculiar case in hg19 chrX
          VCFMutation.logger.info("MEINFO unexpected format in mutation "+mutationStringWithoutFormat+
            " ALU APPROXIMATE LENGTH RETURNED: 300 BASES.")
          300   // approximate length of ALU element. Without further info, this is the best guess.
        } else { // uncovered case
          VCFMutation.logger.error("UNABLE TO DETERMINE THE LENGTH OF VARIANT: "+mutationStringWithoutFormat)
          0
        }
    }

    private def mutationStringWithoutFormat: String ={
      String.join(" ",
        m.chr, m.pos, m.id, m.ref, m.alt, m.info.toString()
      )
    }

  }



}






