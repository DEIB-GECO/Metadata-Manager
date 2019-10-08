package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.BufferedReader
import java.util.regex.Pattern

import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable

/**
 * Created by Tom on ott, 2019
 *
 * Class used to parse VCF files.
 *
 * ASSUMPTIONS:
 * By VCF format specification version 4.2, the file can begin with a set of meta-information lines preceded by characters ##.
 * Next a header line is distinguished by a # starting character and followed by 8 fixed and mandatory columns:
 * CHROM POS ID REF ALT QUAL FILTER INFO.
 * The optional column FORMAT is assumed to be always present (right after INFO as per specifications), otherwise the
 * question arises on the feasibility of the whole integration process.
 *
 */
object VCFInfo {
  import VariantPattern._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Helper class used to extract the different parts of a variant line
   */
  class VariantPattern {
    private var chr = ANY_CHAR_SEQUENCE
    private var pos = ANY_CHAR_SEQUENCE
    private var id = ANY_CHAR_SEQUENCE
    private var ref = ANY_CHAR_SEQUENCE
    private var alt = ANY_CHAR_SEQUENCE
    private var qual = ANY_CHAR_SEQUENCE
    private var filter = ANY_CHAR_SEQUENCE
    private var info = ANY_CHAR_SEQUENCE
    private var format = ANY_CHAR_SEQUENCE
    private val genotypes = ANYTHING

    def get(): Pattern ={
      PatternMatch.createPattern(getRegex())
    }

    def getRegex(): String ={
      "("+chr+")"+BLANK+
      "("+pos+")"+BLANK+
      "("+id+")"+BLANK+
      "("+ref+")"+BLANK+
      "("+alt+")"+BLANK+
      "("+qual+")"+BLANK+
      "("+filter+")"+BLANK+
      "("+info+")"+BLANK+
      "("+format+")"+BLANK+
      "("+genotypes+")"
    }
  }
  object VariantPattern {
    val ANY_CHAR_SEQUENCE = "\\S+"
    val POSSIBLY_EMPTY_CHAR_SEQ = "\\S*"
    val ANYTHING = ".*"
    val BLANK = "\\s+"
    val GT_PHASED_REGEX = "\\d\\|\\d.*|\\d/\\d.*"
    val GT_PHASED_NOT_PRESENT_REGEX = "0/0.*|0\\|0.*"
    val GT_NOT_PRESENT = "0"
  }

  class MutationRepresentation(mutationParts: List[String]) {
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
    def info:String ={
      mutationParts(7)
    }
    def format:String ={
      mutationParts(8)
    }
    def customPart(sampleNum: Int):String ={
      split(mutationParts(7))(sampleNum)
    }
    def genotype(sampleNum: Int): String ={
      val c = customPart(sampleNum)
      c.substring(0, c.indexOf(":"))
    }
    def genotype(sampleName: String, biosamples: List[String]): Unit = {
      genotype(biosamples.indexOf(sampleName))
    }
  }

  /**
   * @param VCFFilePath the path (relative to the project's root dir) to a VCF file complete of genotype information.
   * @return an array of biosamples' names read from the heaser line of the argument VCF file given
   */
  def biosamples(VCFFilePath: String): List[String] = {
    val reader = FileUtil.open(VCFFilePath).get
    // skip meta-information lines
    var headerLine = VCFInfo.skipVCFMetaInfo(reader)
    reader.close()
    // skip mandatory header columns
    val samplesString = headerLine.split("FORMAT", 2)(1).trim
    // create array of biosamples' names
    split(samplesString).toList
  }

  /**
   * @param variantLine a line from the body of a VCF file
   * @return a List of the indices of the biosamples in which the given variant is expressed.
   */
  def samplesPositiveToVariant(variantLine: String): List[Int]  = {
    val genotypes = genotypesFromVariant(variantLine)
    if(isPhasedGenotype(genotypes.head))
      genotypes.indices.filter(i => !genotypes(i).matches(GT_PHASED_NOT_PRESENT_REGEX)).toList
    else
      genotypes.indices.filter(i => !genotypes.startsWith(GT_NOT_PRESENT)).toList
  }

  /**
   * @param variantLine a line from the body of a VCF file
   * @param biosamples the List of biosamples' names
   * @return a List of biosamples' names where the given variant is expressed.
   */
  def samplesPositiveToVariant(variantLine: String, biosamples: List[String]): List[String] ={
    samplesPositiveToVariant(variantLine).map(index => biosamples(index))
  }

  /**
   * Writes a file with the mutations that apply to the given sample, in the same way as they appear in the original file.
   * @param sampleNum number of the sample whose mutations have to be included in the file. This number is the index
   *                  of the sample when getting the list of samples through the method biosamples.
   * @param VCFFilePath the path to the VCF file to analyze.
   * @param outputFilePath the output file where to write the mutations.
   */
  def writeVariantsOfSample(sampleNum: Int, VCFFilePath: String, outputFilePath: String): Unit ={
    val writer = FileUtil.writeReplace(outputFilePath)
    val reader = FileUtil.open(VCFFilePath).get
    VCFInfo.skipVCFMetaInfo(reader)
    var aVariant = reader.readLine()
    val phased = isPhasedGenotype(genotypesFromVariant(aVariant).head)
    logger.debug("IS PHASED ? "+phased)
    // prepare pattern
    val pattern = (new VariantPattern).get()
    // search genotype for argument sampl
    while(aVariant != null){
      val thisSampleGenotype = genotypesFromVariant(aVariant)(sampleNum)
      if(isPositiveGenotype(thisSampleGenotype, phased)) {
//        val variantParts = PatternMatch.matchParts(aVariant, pattern)
        writer.write(aVariant)
        writer.newLine()
      }
      aVariant = reader.readLine()
    }
    reader.close()
    writer.close()
  }

  def TESTSamplesPositiveToVariant(): Unit ={
    val filePath = "Example/examples_meta/1kGenomes/GRCh38/Downloads/ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf/ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf"
    val samples = biosamples(filePath)
    val reader = FileUtil.open(filePath).get
    val variant = skipVCFHeader(reader)
    reader.close()
    val positiveSamples = samplesPositiveToVariant(variant, samples)
    logger.debug("POSITIVE SAMPLES: ")
    positiveSamples.foreach(sam => logger.debug(sam))
  }

  def TESTwriteVariantsOfSample(): Unit ={
    val VCFFilePath = "Example/examples_meta/1kGenomes/GRCh38/Downloads/ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf/ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf"
    val output = "Example/examples_meta/1kGenomes/GRCh38/Transformations/chrX_sample_0.vcf"
    writeVariantsOfSample(0, VCFFilePath, output)
  }

  def skipVCFMetaInfo(reader: BufferedReader): String ={
    var headerLine = reader.readLine()
    while(headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    headerLine
  }

  def skipVCFHeader(reader: BufferedReader): String ={
    var firstVariantLine = reader.readLine()
    while(firstVariantLine.startsWith("#")) {
      firstVariantLine = reader.readLine()
    }
    firstVariantLine
  }

  /**
   * Method used to separate the components of a variant line. It makes the assumtion that the attributes are separated
   * by a tab or a whitespace character.
   * @param VCFBodyline a line belonging to the body of the VCF file.
   * @return an array of strings, each one being a sequence of characters in the original line
   */
  private def split(VCFBodyline: String): Array[String] ={
    VCFBodyline.split("\\s+")
  }

  /**
   * @param variantLine a line describing a mutation in a VCF file
   * @return a List of genotype information. Each genotype information is related to a biosample. You can have the
   *         list of biosamples extracted from a VCF file by calling the method biosamples.
   */
  def genotypesFromVariant(variantLine: String): List[String] = {
    val genotypesString = PatternMatch.matchParts(variantLine, (new VariantPattern).get()).last
    VCFInfo.split(genotypesString).toList
  }

  /**
   * Phased genotypes are expressed as a couple of numbers separated by a vertical bar | or a slash /.
   * @param genotypeValue a String representing the genotype of a sample
   * @return true if the genotype is expressed as phased, false otherwise.
   */
  def isPhasedGenotype(genotypeValue: String):Boolean ={
//    val phasedVerticalBar = "\\d\\|\\d.*"
//    val phasedSlash = "\\d/\\d.*"
    genotypeValue.matches(GT_PHASED_REGEX)
  }

  /**
   * Tells if the genotypeValue passed as argument reports that the mutation is present
   */
  def isPositiveGenotype(genotypeValue: String, phased: Boolean): Boolean ={
    if(phased)
      !genotypeValue.matches(GT_PHASED_NOT_PRESENT_REGEX)
    else
      !genotypeValue.startsWith(GT_NOT_PRESENT)
  }
}
