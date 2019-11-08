package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, File}

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FileUtil, ManyToFewMap}
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.OneKGTransformer._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Created by Tom on ott, 2019
 */
class TestTransf extends Transformer {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var individualsMetadata: ManyToFewMap[String, List[String]] = _
  private var transformedVCFs: ListBuffer[String] = ListBuffer.empty[String]

  /**
   * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
   *
   * @param source           source where the files belong to.
   * @param originPath       path for the  "Downloads" folder
   * @param destinationPath  path for the "Transformations" folder
   * @param originalFilename name of the original file .json/.gz
   * @param filename         name of the new file .meta/.bed
   * @return List(fileId, filename) for the transformed files.
   */
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    println("called with origin "+originalFilename+" and target "+filename)
    val sourceFileName = removeCopyNumber(originalFilename)
    val targetFileName = filename
    val sourceFilePath = originPath+File.separator+sourceFileName
    val targetFilePath = destinationPath+File.separator+targetFileName

    println("ORIGIN PATH: "+originPath+File.separator)
    return false

    if (filename.endsWith(".gdm")) {
      if(!transformedVCFs.contains(sourceFileName)){
        new VCFAdapter(sourceFilePath).appendAllMutationsBySample(destinationPath+File.separator)
        transformedVCFs += sourceFileName
      }
      true
    } else if (filename.endsWith(".meta")) {
      val sampleName = removeExtension(removeExtension(targetFileName))
      val writer = FileUtil.writeReplace(targetFilePath).get // throws exception if fails
      // gender
      writeMetadata(writer, onNewLine = true, "gender", Try(individualsMetadata.getFirstOf(sampleName).get.head).getOrElse(MISSING_VALUE))
      writer.newLine()  // prevents TransformerStep from concatenating the attribute "manually_curated__local_file_size" on last line
      writer.close()
      true
    } else
      false
  }

  /**
   * by receiving an original filename returns the new GDM candidate name(s).
   * The region file must be before than related meta file
   *
   * @param filename original filename
   * @param dataset  dataset where the file belongs to
   * @param source   source where the files belong to.
   * @return candidate names for the files derived from the original filename.
   */
  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
    println("candidate name: "+filename)

    val downloadDirPath = s"${dataset.fullDatasetOutputFolder}${File.separator}Downloads${File.separator}"
    println(dataset.fullDatasetOutputFolder+File.separator+"Downloads"+File.separator+" == "+downloadDirPath+" ?")

    if(filename == "ALL_0.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf.gz"){
      val inputFile = DatasetInfo.getDownloadDir(dataset)+"ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased_short.vcf"
      val VCF = new VCFAdapter(inputFile)
      VCF.biosamples.map(sample => s"$sample.gdm").toList
    }
    else if(filename == "1000genomes_0.sequence.index") {
      individualsMetadata = getIndividualDetailsMetadata(dataset)
      individualsMetadata.keys.toList.map(sample => s"$sample.gdm.meta")
    } else
      List.empty[String]
  }

  def getIndividualDetailsMetadata(dataset: Dataset): ManyToFewMap[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+"integrated_call_samples_v2.20130502.ALL.ped").get
    // skip header line
    reader.readLine()
    readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(4), mapByColumnIndex = 1)
      .transformValues((_, valueList) => {
        valueList.flatMap(row => List(if(row.head == "1") "male" else "female"))
      })
  }

  def readTSVFileAsManyToFewMap(reader: BufferedReader, interestingColumnsIdx: List[Int], mapByColumnIndex: Int): ManyToFewMap[String, List[String]] = {
    val mtfMap = new ManyToFewMap[String, List[String]]
    // fill the map
    FileUtil.scanFileAndClose(reader, line => {
      try {
        if(!line.trim.isEmpty) {
          val lineFields = line.split("\t")
          val values: List[String] = interestingColumnsIdx.map(i => lineFields(i))
          mtfMap.add(lineFields(mapByColumnIndex), values)
        }
      } catch {
        case e: Exception =>
          logger.error("TSV PARSER ERROR. SKIPPED LINE: "+line, e)
      }
    })
    mtfMap
  }

}
