package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, BufferedWriter}

import it.polimi.genomics.metadata.util.vcf.{HeaderMetaInformation, VCFMutation}
import it.polimi.genomics.metadata.util.{ApproximateReadProgress, AsyncFilesWriter, FileUtil}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{immutable, mutable}
import scala.util.Try

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

  val biosamples: immutable.IndexedSeq[String] = biosamples(VCFFilePath)
  private var numberOfLinesInFile: Option[Long] = None
  private val headerMeta = new HeaderMetaInformation(VCFFilePath)
  private val shortName = Try(FileUtil.getFileNameFromPath(VCFFilePath).split("\\.")(1)).getOrElse(FileUtil.getFileNameFromPath(VCFFilePath))
  private var mutationEnriched: KGMutation => KGMutation = (m:KGMutation) => m

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
    if(!biosamples.contains(sampleName))
      false // the sample isn't listed in this VCF
    else {
      val writer = FileUtil.writeAppend(toFilePath).get  // let it throw exception if failed
      var targetFileEmpty = FileUtil.size(toFilePath) == 0
      val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
      advanceAndGetHeaderLine(reader)   // advance reader and skip header line
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).map(mutationEnriched)
          .foreach(mutation => {
            val formatOfSample = mutation.format(sampleName, biosamples)
            // appends to the sample's region file if positive
            if (mutation.isSampleMutated(formatOfSample)) {
              val outputLine = mutationPrinter.formatMutation(mutation, Some(formatOfSample))
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

  def getSamplesWithMutation(mutation: KGMutation): immutable.IndexedSeq[String] ={
    biosamples.filter(sample => {
      mutation.isSampleMutated(mutation.format(sample, biosamples))
    })
  }

  /////////////////////////////////   WHOLE FILE -> ALL SAMPLES   ///////////////////////////////////////////////////////

  def appendAllMutationsBySample(outputDirectoryPath: String): Unit = {
    var writers:mutable.AnyRefMap[String, BufferedWriter] = mutable.AnyRefMap.empty
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    advanceAndGetHeaderLine(reader)   // advance reader and skip header line
    try {
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read and transform each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).map(mutationEnriched)
          .foreach(mutation => {
            // get the format of all mutated samples
            biosamples.map(sample => sample -> mutation.format(sample, biosamples))
              .filter(sampleAndFormat => mutation.isSampleMutated(sampleAndFormat._2)).foreach { sampleAndFormat =>
              // append this mutation to the region files of the affected samples
              val outputLine = mutationPrinter.formatMutation(mutation, Some(sampleAndFormat._2))
              if (writers.get(sampleAndFormat._1).isEmpty) {
                val outputFile = outputDirectoryPath + sampleAndFormat._1 + ".gdm"
                val writer = FileUtil.writeAppend(outputFile, startOnNewLine = true).get
                writers += (sampleAndFormat._1 -> writer)
                writer.write(outputLine)
              } else {
                val writer = writers(sampleAndFormat._1)
                writer.newLine()
                writer.write(outputLine)
              }
            }
          })
      }, setupReadProgressCanary(VCFFilePath))
    } finally {
      writers.values.foreach(writer => writer.close())
      reader.close()
    }
  }

  def appendAllMutationsBySampleRunnable(outputDirectoryPath: String, asyncWriter: AsyncFilesWriter): Runnable = {
    new Runnable {
      override def run(): Unit = {
//        asyncWriter.addJob(this)   // job is registered by the user of this runnable before this runnable starts
        asyncWriter.addTargetFiles(biosamples, biosamples.map(sampleName => outputDirectoryPath + sampleName + ".gdm"))
        val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
        advanceAndGetHeaderLine(reader)   // advance reader and skip header line
        try {
          FileUtil.scanFileAndClose(reader, mutationLine => {
            // read and transform each mutation
            VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).map(mutationEnriched)
              .foreach(mutation => {
                // get the format of all mutated samples
                biosamples.map(sample => sample -> mutation.format(sample, biosamples))
                  .filter(sampleAndFormat => mutation.isSampleMutated(sampleAndFormat._2)).foreach { sampleAndFormat =>
                  // append this mutation to the region files of the affected samples
                  val outputLine = mutationPrinter.formatMutation(mutation, Some(sampleAndFormat._2))
                  asyncWriter.write(outputLine, sampleAndFormat._1)
                }
              })
          }, setupReadProgressCanary(VCFFilePath))
        } finally {
          reader.close()
          asyncWriter.removeJob(this)
        }
      }

      override def equals(obj: Any): Boolean = {
        obj.isInstanceOf[Runnable] && obj.asInstanceOf[Runnable].toString == toString
      }

      override def toString: String = {
        shortName
      }
    }
  }

  def appendAllMutations(outputFilePath: String): Unit = {
    val writer = FileUtil.writeAppend(outputFilePath, startOnNewLine = true).get
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    advanceAndGetHeaderLine(reader)   // advance reader and skip header line
    try {
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read and transform each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).map(mutationEnriched)
          .foreach(mutation => {
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
   * @return an array of biosamples' names read from the header line of the argument VCF file given
   */
  private def biosamples(VCFFilePath: String): immutable.IndexedSeq[String] = {
    val reader = FileUtil.open(VCFFilePath).get
    val headerLine = advanceAndGetHeaderLine(reader)
    reader.close()
    // skip mandatory header columns
    val samplesString = headerLine.split("FORMAT", 2)(1).trim
    // create array of biosamples' names
    samplesString.split(VCFMutation.COLUMN_SEPARATOR_REGEX).map(x => x.trim).toIndexedSeq
  }

  /**
   *
   * @return an Option containing the first KGMutation occurring in this VCF.
   */
  def getFirstMutation:Option[KGMutation] ={
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    advanceAndGetHeaderLine(reader)   // advance reader and skip header line
    val firstLine = reader.readLine()
    if(firstLine!=null)
      Some(VCFMutation.splitOnMultipleAlternativeMutations(firstLine, headerMeta)
        .map(KGMutation.apply).map(mutationEnriched).head)
    else None
  }

  /**
   * Allows to define a function to be applied to each KGMutation before it is printed and before any of its attribute
   * is read by this class.
   * @param f a function receiving as input a KGMutation and returning a KGMutation.
   * @return this
   */
  def enrichMutationsBeforeWriting(f: KGMutation => KGMutation): VCFAdapter ={
    mutationEnriched = f
    this
  }

  /**
   * Prepares an instance of ApproximateReadProgress to receive progress updates while processing files very large in size.
   * Progress are not logged through a logger instance as they're useful only for a user who wants to have an estimate
   * of the time left to finish.
   * This estimate is just a rough approximation and requires some more seconds before the file processing can begin in
   * order to compute the necessary parameters.
   */
  private def setupReadProgressCanary(fullFilePath: String): Option[ApproximateReadProgress] ={
    if(numberOfLinesInFile.isEmpty){
      logger.info("COUNTING LINES OF FILE " + shortName)
      numberOfLinesInFile = FileUtil.countLines(fullFilePath).toOption
    }
    if(numberOfLinesInFile.isEmpty){
      logger.info("COUNT OF LINES IN FILE "+fullFilePath+" FAILED")
      None
    } else {
      logger.info("COUNTED "+numberOfLinesInFile.get+" LINES IN "+shortName)
      Some(new ApproximateReadProgress(
        numberOfLinesInFile.get,
        progressNotificationStep = 10,
        ApproximateReadProgress.simpleProgressNotification(filename = shortName, logger = Some(logger))))
    }
  }

}







