package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, File}

import it.polimi.genomics.metadata.util.FileUtil
import it.polimi.genomics.metadata.util.vcf.{VCFFormat, VCFInfo, VCFMutation}

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
class VCFHelper(VCFFilePath: String) {

  val biosamples: List[String] = biosamples(VCFFilePath)
  val MISSING_VALUE_CODE = "*"

  /** TEST METHOD  */
  def TEST: Unit = {
    val reader = FileUtil.open(VCFFilePath).get
    val aVariant = advanceAndGetFirstMutation(reader)
    reader.close()
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
    println("HG01770 has genotype "+format.genotype+". Is it mutated ? "+format.hasMutation)

    format = mutation.format("HG01124", biosamples)
    println("HG01124 has genotype "+format.genotype+". Is it mutated ?"+format.hasMutation)
  }


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
    biosamples.indexOf(sampleName) match {
      case -1 => // the sample isn't listed in this VCF
        false
      case index =>
        val writer = FileUtil.writeAppend(toFilePath, startOnNewLine = true).get  // let it throw exception if failed
        val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
        advanceAndGetHeaderLine(reader)   // advance reader and skip header line
        FileUtil.scanFileAndClose(reader, mutationLine => {
          val mutation = new VCFMutation(mutationLine)
          val formatOfSample = mutation.format(sampleName, biosamples)
          if(formatOfSample.hasMutation){
            val infoFields = mutation.info
            writer.write(OneKGTransformer.tabber(
              mutation.chr,
              mutation.pos,
              //TODO mutation.end
              "+",   // strand (see class documentation for explanations)
              mutation.id,
              mutation.ref,
              mutation.alt,
              infoFields.get(VCFInfo.ALLELE_FREQUENCY).getOrElse(MISSING_VALUE_CODE)
              //TODO go on with other attributes based on the desired schema for region data
            ))
          }
        })
        writer.close()
        true
    }
  }

  ////////////////////////////////////  VARIANT -> SAMPLES   ///////////////////////////////////////////////////////////

  def getSamplesWithMutation(mutationLine: String): List[String] ={
    val mutation = new VCFMutation(mutationLine)
    biosamples.filter(sample => {
      mutation.format(sample, biosamples).hasMutation
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
}






