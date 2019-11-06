package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, BufferedWriter}

import it.polimi.genomics.metadata.util.vcf.VCFMutation.MutationProperties
import it.polimi.genomics.metadata.util.vcf.{MetaInformation, VCFMutation}
import it.polimi.genomics.metadata.util.{FileUtil, RoughReadProgress, XMLHelper}
import org.slf4j.{Logger, LoggerFactory}

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
class VCFAdapter(VCFFilePath: String, mutationPrinter:MutationPrinterTrait = new SimplePrinter) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val biosamples: List[String] = biosamples(VCFFilePath)
  private var numberOfLinesInFile: Option[Long] = None
  MetaInformation.updatePropertiesFromMetaInformationLines(VCFFilePath)

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
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine).map(OKGMutation.apply).foreach(mutation => {
          val formatOfSample = mutation.format(sampleName, biosamples)
          // appends to the sample's region file if positive
          if (formatOfSample.isMutated) {
            val outputLine = mutationPrinter.formatMutation(mutation)
            if (!targetFileEmpty)
              writer.newLine()
            else
              targetFileEmpty = false
            writer.write(outputLine)
          }
        })
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

  /////////////////////////////////   WHOLE FILE -> ALL SAMPLES   ///////////////////////////////////////////////////////

  def appendAllMutationsBySample(outputDirectoryPath: String): Unit = {
    var writers:Map[String, BufferedWriter] = Map.empty
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    advanceAndGetHeaderLine(reader)   // advance reader and skip header line
    try {
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read and transform each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine).map(OKGMutation.apply).foreach(mutation => {
          val outputLine = mutationPrinter.formatMutation(mutation)
          // find the affected samples (there's always at least one)
          val samplesWithMutation = biosamples.filter(sample => {
            mutation.format(sample, biosamples).isMutated
          })
          // append this mutation to the region files of the affected samples
          samplesWithMutation.foreach(sampleName => {
            val outputFile = outputDirectoryPath + sampleName + ".gdm"
            if (writers.get(sampleName).isEmpty) {
              val writer = FileUtil.writeAppend(outputFile).get
              writers += (sampleName -> writer)
              if (FileUtil.size(outputFile) != 0)
                writer.newLine()
              writer.write(outputLine)
            } else {
              val writer = writers(sampleName)
              writer.newLine()
              writer.write(outputLine)
            }
          })
        })
      }, setupReadProgressCanary(VCFFilePath))
    } finally {
      writers.values.foreach(writer => writer.close())
      reader.close()
    }
  }

  def appendAllMutations(outputFilePath: String): Unit = {
    val writer = FileUtil.writeAppend(outputFilePath, startOnNewLine = true).get
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    advanceAndGetHeaderLine(reader)   // advance reader and skip header line
    try {
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read and transform each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine).map(OKGMutation.apply).foreach(mutation => {
          val outputLine = mutationPrinter.formatMutation(mutation)
          writer.write(outputLine)
          writer.newLine()
        })
      }, setupReadProgressCanary(VCFFilePath))
    } finally {
      writer.close()
      reader.close()
    }
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
    if(numberOfLinesInFile.isDefined) {
      println("SCANNING "+numberOfLinesInFile.get+" LINES FROM "+fullFilePath)
      Some(new RoughReadProgress(numberOfLinesInFile.get, 10, RoughReadProgress.notifyProgress))
    } else {
      println("COUNTING LINES OF FILE: " + fullFilePath)
      FileUtil.countLines(fullFilePath) match {
        case Failure(_) =>
          println("COUNT OF LINES IN FILE FAILED")
          None
        case Success(value) =>
          numberOfLinesInFile = Some(value)
          println("COUNTED "+value+" LINES")
          Some(new RoughReadProgress(value, 10, RoughReadProgress.notifyProgress))
      }
    }
  }

}







