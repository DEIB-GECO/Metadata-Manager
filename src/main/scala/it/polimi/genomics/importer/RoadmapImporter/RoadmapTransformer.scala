package it.polimi.genomics.importer.RoadmapImporter

import java.io.{File, _}
import java.util.regex.Pattern

import com.github.tototoshi.csv.{CSVReader, CSVWriter, DefaultCSVFormat}
import it.polimi.genomics.importer.DefaultImporter.utils.Unzipper
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import scala.util.matching.Regex

class RoadmapTransformer  extends GMQLTransformer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val patternPeakFile: Regex = """.*\.narrowPeak|.*\.gappedPeak|.*\.broadPeak|.*\.peaks\.bed|.*\.broad\.bed""".r
  val patternRNAexpFile: Regex = """.*\.pc|.*\.nc|.*\.rb""".r
  val patternRNAgenFile: Regex = """.*pc\.bed|.*nc\.bed|.*rb\.bed""".r
  val patternRNAexpArch: Regex = """.*\.pc.gz|.*\.nc.gz|.*\.rb.gz""".r
  val patternDMRFile: Regex = """.*_DMRs_v2\.bed""".r
  val patternMetadata: Regex = """.*\.meta""".r

  /**
    * recieves .gz files and transform them invalid regions files and generate the corresponding .meta files
    *
    * @param source           source where the files belong to.
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file
    * @param filename         name of the new file
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: GMQLSource, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val fileDownloadPath = originPath + File.separator + originalFilename
    val fileTransformationPath = destinationPath + File.separator + filename
    val splitPath = destinationPath.split(Pattern.quote(File.separator))
    val sourceID = FileDatabase.sourceId(source.name)
    var datasetName = ""
    for (dataset <- source.datasets) {
      if (dataset.outputFolder == splitPath(splitPath.length-2))
        datasetName = dataset.name
    }
    val datasetID = FileDatabase.datasetId(sourceID, datasetName)
    val addMetadata = new ListBuffer[(String, String)]()

    if (originalFilename.endsWith(".gz")) { //all the Roadmap files are provided inside a GZip archive
      var isTransformationDone: Boolean = true //false if an error occurs during the transformation
      //detect the type of data from the candidate name of the transformed file and apply different transformation
      filename match {
        case patternMetadata() =>
          //generate some additional common metadata that require information from source or dataset
          var metaUrl: String = ""
          var infoUrl: String = ""
          for (dataset <- source.datasets) {
            if (dataset.outputFolder == splitPath(splitPath.length - 2)) {
              if (dataset.parameters.exists(_._1 == "spreadsheet_url"))
                metaUrl = dataset.parameters.filter(_._1 == "spreadsheet_url").head._2
              if (dataset.parameters.exists(_._1 == "info_url"))
                infoUrl = dataset.parameters.filter(_._1 == "info_url").head._2
            }
          }
          addMetadata += (("metadata_url", metaUrl))
          addMetadata += (("file_id", filename.substring(0, filename.lastIndexOf("."))))
          addMetadata += (("file_size", new File(fileTransformationPath.substring(0, fileTransformationPath.lastIndexOf("."))).length.toString))
          if (originalFilename.matches(patternRNAexpArch.regex))
            addMetadata += (("data_url", FileDatabase.getFileUrl(originalFilename, datasetID, STAGE.DOWNLOAD) + "; " +
              FileDatabase.getFileUrl(originalFilename.replace("N", "RPKM"), datasetID, STAGE.DOWNLOAD) + "; " + infoUrl))
          else
            addMetadata += (("data_url", FileDatabase.getFileUrl(originalFilename, datasetID, STAGE.DOWNLOAD)))
          //generation of the .meta file
          val metaGenOutcome = Try(metaGen(filename, originPath, destinationPath, addMetadata))
          if (metaGenOutcome.isSuccess) {
            logger.info("metaGen: " + filename + " DONE")
          }
          else {
            isTransformationDone = false
            logger.warn("metaGen: " + filename + " FAILED", metaGenOutcome.failed.get)
          }
        case patternPeakFile() =>
          if (Unzipper.unGzipIt(fileDownloadPath, fileTransformationPath)) {
            logger.info("unGzipping: " + originalFilename + " DONE")
            //some region files need to be modified
            if (filename.matches(""".*\.all.peaks.bed|.*\.broad.bed""")) {
              val hotspotAdjustmentOutcome = Try(hotspotAdjustment(filename, destinationPath))
              if (hotspotAdjustmentOutcome.isSuccess)
                logger.info("hotspotAdjustment: " + filename + " DONE")
              else {
                isTransformationDone = false
                logger.warn("hotspotAdjustment: " + filename + " FAILED", hotspotAdjustmentOutcome.failed.get)
              }
            }
          }
          else {
            logger.warn("unGzipping: " + originalFilename + "FAILED")
            isTransformationDone = false
          }
        case patternRNAgenFile() =>
          //region files are generated starting from multiple downloaded files
          val geneExpTransformationOutcome = Try(geneExpTransformation(destinationPath, filename, originPath))
          if (geneExpTransformationOutcome.isSuccess) {
            logger.info("geneExpTransformation: " + filename + " DONE")
          }
          else {
            isTransformationDone = false
            logger.warn("metaGen: " + filename + " FAILED", geneExpTransformationOutcome.failed.get)
          }
        case patternDMRFile() =>
          if (Unzipper.unGzipIt(fileDownloadPath, fileTransformationPath)) {
            logger.info("unGzipping: " + originalFilename + " DONE")
            val DMRAdjustmentOutcome = Try(DMRAdjustment(filename, destinationPath))
            // all the region files are modified
            if (DMRAdjustmentOutcome.isSuccess)
              logger.info("DMRAdjustment: " + filename + " DONE")
            else {
              isTransformationDone = false
              logger.warn("DMRAdjustment: " + filename + " FAILED", DMRAdjustmentOutcome.failed.get)
            }
          }
          else {
            logger.warn("unGzipping: " + originalFilename + "FAILED")
            isTransformationDone = false
          }
        case _ =>
          logger.warn(s"File $filename format not supported.")
          isTransformationDone = false
      }
      isTransformationDone
    }
    else {
      logger.warn("format of " + originalFilename + " not supported")
      false
    }
  }

  /**
    * generate a .meta file in tsv format containing metadata attributes name and value associated to an input file
    *
    * @param inPath   path for the  "Downloads" folder
    * @param outPath  path for the "Transformations" folder
    * @param fileName name of the input file for which the .meta file must be generated
    * @return boolean asserting if the meta file is correctly generated
    */
  def metaGen(fileName: String, inPath: String, outPath: String, additionalMeta: ListBuffer[(String, String)] = ListBuffer()): Unit = {
    val dataFileName = fileName.substring(0, fileName.lastIndexOf("."))
    val sheet1: File = new File(inPath + File.separator + "jul2013.roadmapData.qc_Consolidated_EpigenomeIDs_summary_Table.csv")
    val sheet2: File = new File(inPath + File.separator + "jul2013.roadmapData.qc_Consolidated_EpigenomeIDs_QC.csv")
    val tempSheet: File = new File(inPath + File.separator + "tempSheet.csv")
    val reader1: CSVReader = CSVReader.open(sheet1)

    //load first sheet in memory
    val firstSheet: List[List[String]] = reader1.all()

    //merge first line (header) with the second one (sub-header) of the first sheet
    val newHeader: List[String] = (firstSheet.head zip firstSheet(1)).map(tuple => {
      if (firstSheet.head.count(_ == tuple._1) > 1 && tuple._2.nonEmpty)
        tuple._1 + "__" + tuple._2
      else
        tuple._1
    })

    //write in a temporary file the new header and the first sheet
    val writer: CSVWriter = CSVWriter.open(tempSheet)
    writer.writeRow(newHeader)
    writer.writeAll(firstSheet)
    writer.close()
    reader1.close()

    //load in memory temp sheet and second sheet
    val reader3: CSVReader = CSVReader.open(tempSheet)
    val reader2: CSVReader = CSVReader.open(sheet2)

    //load the headers
    val listMaps1: List[Map[String, String]] = reader3.allWithHeaders()
    val listMaps2: List[Map[String, String]] = reader2.allWithHeaders()

    //extract eid, mark and format from the file name
    val eid: String = dataFileName.split("_")(0)
    val mark: String = extractMark(dataFileName)
    val nameComp = dataFileName.split("\\.")
    val format: String = nameComp(nameComp.length-1)

    val manCuratedMeta = new mutable.TreeSet[(String, String)]

    //select line in first sheet corresponding to the input file
    val eid_index: Int = listMaps1.indexWhere(_ ("Epigenome ID (EID)") == eid)

    //write attribute name and value on .meta file

    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outPath + File.separator + fileName))))) {
      writer => {
        if (eid_index <0) {
          //no data available for this eid in the datasheet, training to add some manually inferred custom information
          eid match {
            case "E000" =>
              val inferredMetadata = Map(("Epigenome ID (EID)", eid),
                ("Standardized Epigenome name", "Universal Human Reference RNA"),
                ("Epgenome Mnemonic", "HUR"),
                ("Comments", "Agilent's Universal Human Reference RNA is composed of total RNA from 10 human cell lines. " +
                  "The reference RNA is designed to be used as a reference for expression profiling experiments."))
              mapToFile(inferredMetadata, writer, "epi")
            case "E064" | "E060" =>
              val inferredMetadata = Map(("Comments", "Rejected due to data quality issues."),
                ("Epigenome ID (EID)", eid))
              mapToFile(inferredMetadata, writer, "epi")
          }
        }
        else
          //write the metadata associated with the epigenome of th file
          mapToFile(listMaps1(eid_index).filterKeys(key => !(key.split("__").length > 1 && key.split("__")(1) != mark)), writer, keyPrefix =  "epi",
            valueCorrections = List((".*AGE_Post_Birth_in_YEARS_or_Fetal_in_GESTATIONAL_WEEKS_or_CELL_LINE_CL.*", ageCorrector)))
        dataFileName match { //add type specific metadata
            case patternPeakFile() =>
              //this metadata are duplicated and have been written with the epigenome metadata
              val duplicates = List("EID", "E-Mnemonic", "Standardised epigenome name",
                "Epigenome name (from EDACC Release 9 directory)", "NSC (Signal to noise)", "RSC (Phantom Peak)", "NREADS")
              if (eid_index > 0)
                for (m <- listMaps2)
                  //write the metadata associated with the experiment, excluding that one already written
                  if (m("EID") == listMaps1(eid_index)("Epigenome ID (EID)") && m("MARK") == mark)
                    mapToFile(m, writer, keyPrefix =  "exp", skipList = duplicates)
              //adding type-specific manually curated files
              if (mark=="DNase") {
                manCuratedMeta += (("data_type", "DNase-seq"))
                manCuratedMeta += (("feature", "open chromatin"))
              }
              else {
                manCuratedMeta += (("data_type", "ChIP-seq"))
                manCuratedMeta += (("feature", "histone modification"))
              }
              if (format == "bed") {
                manCuratedMeta += (("peak_caller", "HOTSPOT"))
                if (nameComp.length > 1) {
                  manCuratedMeta += (("region_type", if (nameComp(nameComp.length-2).toLowerCase == "broad") "broad" else "narrow"))
                  if(nameComp.length > 2 && nameComp(nameComp.length-3) == "01")
                    manCuratedMeta += (("fdr_threshold", "0.01"))
                  else
                    manCuratedMeta += (("fdr_threshold", "none"))
                }
              }
              else
                manCuratedMeta += (("peak_caller", "MACS2"))
            case patternRNAgenFile() =>
              manCuratedMeta += (("data_type", "RNA-seq"))
              manCuratedMeta += (("feature", "gene expression"))
              if (dataFileName.contains("exon") || dataFileName.contains("exn")) {
                dataFileName match {
                  case s if s.matches(".*pc.bed") => manCuratedMeta += (("RNA_expression_region", "protein coding exons"))
                  case s if s.matches(".*nc.bed") => manCuratedMeta += (("RNA_expression_region", "protein non-coding exons"))
                  case s if s.matches(".*rb.bed") => manCuratedMeta += (("RNA_expression_region", "ribosomal gene exons"))
                }
              }
              else if (dataFileName.contains("intronic")) {
                manCuratedMeta += (("RNA_expression_region", "intronic protein-coding RNA elements"))
              }
              else {
                dataFileName match {
                  case s if s.matches(".*pc.bed") => manCuratedMeta += (("RNA_expression_region", "protein coding genes"))
                  case s if s.matches(".*nc.bed") => manCuratedMeta += (("RNA_expression_region", "non-coding RNAs"))
                  case s if s.matches(".*rb.bed") => manCuratedMeta += (("RNA_expression_region", "ribosomal genes"))
                }
              }
            case patternDMRFile() =>
              val methTech = dataFileName.split("_")(1)
              manCuratedMeta += (("data_type", "DMR"))
              manCuratedMeta += (("methylation_technique", methTech))
              manCuratedMeta += (("feature", "DNA methylation"))
            case _ => logger.warn(s"File $dataFileName format not supported.")
          }

        //add metadata in common to all files
        if(eid_index >= 0 && listMaps1(eid_index).keys.toList.contains("AGE\n(Post Birth in YEARS/ Fetal in GESTATIONAL WEEKS/CELL LINE CL) ")) {
          val age = listMaps1(eid_index)("AGE\n(Post Birth in YEARS/ Fetal in GESTATIONAL WEEKS/CELL LINE CL) ")
          age match {
            case _ if age.contains("CL") => manCuratedMeta += (("life_stage", "cell line"))
            case _ if age.contains("Y") => manCuratedMeta += (("life_stage", "born"))
            case _ if age.contains("GW") => manCuratedMeta += (("life_stage", "fetal"))
            case _ =>
          }
        }
        manCuratedMeta += (("format", if (format == "bed") format.toUpperCase else format))
        manCuratedMeta += (("assembly", "hg19"))
        additionalMeta.foreach(kv => manCuratedMeta += ((kv._1, kv._2)))

        //actual writing of the manually curated metadata sorted in alphanumeric order
        for ( (k,v) <- manCuratedMeta)
          writer.write(s"manually_curated__$k\t$v\n")
      }
    }
    reader2.close()
    reader3.close()
    tempSheet.delete
  }

  /**
    * extract the mark from a Roadmap consolidated peak file
    *
    * @param fileName         name of the new file
    * @return String containing the mark in the file name
    */
  def extractMark(fileName: String): String = {
    //val core: String = fileName.split("-")(1).split("\\.").dropRight(1).mkString(".")
    fileName match {
      case patternPeakFile() =>
        val core = fileName.split("_")(1).split("\\.")
        if(core(0) == "H2A")
          core(0) + "." + core(1)
        else
          core(0)
      case patternRNAgenFile() => "RNA-seq"
      case patternDMRFile() =>
        val fileNameSplit = fileName.split("_")
        fileNameSplit(fileNameSplit.length-3)
    }
  }

  /**
    * perform an operation over a resources and close it
    *
    * @param resource        a closeable resources to handle
    * @param block           an operation to perform over the resource
    * @return closed resources
    */
  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }

  /**
    * write a map over a writable resources in tsv; each line contains a key-value pair
    *
    * @param map              map to write
    * @param writer           resources where the map is write
    * @param keyPrefix        prefix to add to the keys, can be omitted
    * @param keyPostfix       postfix to add to the keys, can be omitted
    * @return String containing the mark in the file name
    */
  def mapToFile(map: Map[String, String], writer: Writer, keyPrefix: String = "", keyPostfix: String = "", skipList: List[String] = List(),
                valueCorrections: List[(String, (String, String, Writer) => Unit)] = List()): Unit = {
    for ((k, v) <- map.toSeq.sortBy(_._1))
      if (!(k.isEmpty || v.isEmpty || v.toLowerCase.matches("na|n/a") || skipList.contains(k)))  {
        //the name of the attribute is transformed in a java valid key (alphanumeric string)
        var k_enriched = k.filterNot("\n".toSet).replaceAll("\\s+$", "")
          .replace("%", "_perc_")
          .replace("+", "_plus_")
          .replace("/", "_or_")
          .replace("(", "_")
          .replace(") ", "_")
          .replace(" ", "_")
          .replaceAll("_+", "_")
          .replaceAll("[^A-Za-z0-9_]", "")
        k_enriched = if (k_enriched.startsWith("_")) k_enriched.substring(1)
        else k_enriched
        k_enriched = if (k_enriched.endsWith("_")) k_enriched.substring(0,k_enriched.length-1)
        else k_enriched
        k_enriched = k_enriched.capitalize
        //add the optional prefix and postix
        if (keyPrefix != "")
          k_enriched = keyPrefix + "__" + k_enriched
        if (keyPostfix != "")
          k_enriched = k_enriched + "__" + keyPostfix
        //some additional correction over the metadata can be applied
        var isCorrected = false
        for ((regex, corrector) <- valueCorrections) {
          if (k_enriched.matches(regex)) {
            isCorrected = true
            corrector(k_enriched, v, writer)
          }
        }
        if(!isCorrected)
          writer.write(s"$k_enriched\t$v\n") //else the standard write of the metadata is performed
      }
  }

  /**
    * write the metadata AGE after the conversion of all the value in weeks or "unknown" where possible
    *
    * @param key          name of the attribute
    * @param value        value of the attribute
    * @param writer       writer of the file
    */
  def ageCorrector(key: String, value: String, writer: Writer): Unit = {
    value match{
      case _ if value.contains("CL") =>
      case _ if value.toLowerCase().contains("fetus") =>
        writer.write(s"$key\tunknown\n")
      case _ if value.contains("Y") =>
        val valueSplit = value.split(", |,|\\. |\\.")
        for(i <- valueSplit.indices) {
          valueSplit(i) = Try((valueSplit(i).dropRight(1).toInt * 52).toString).getOrElse("unknown")
        }
        writer.write(s"$key\t${valueSplit.mkString(", ")}\n")
      case _ if value.contains("GW") =>
        val valueSplit = value.split(", |,|\\. |\\.")
        for(i <- valueSplit.indices) {
          valueSplit(i) = Try((valueSplit(i).dropRight(2).toInt * 52).toString).getOrElse("unknown")
        }
        writer.write(s"$key\t${valueSplit.mkString(", ")}\n")
      case _ =>
    }
  }

  /**
    * generate a gene expression region file from the tabular expression quantification files (N and RPKM) and the
    * gene_info file
    *
    * @param filePath       where the tabular gene expression quantification files is located and where the new file
    *                       is placed
    * @param fileName       name of the file to generate
    * @param infoPath       location of the gene_info file
    */
  def geneExpTransformation(filePath: String, fileName: String, infoPath: String): Unit = {
    //load gene_info file in memory
    var geneMap  = new HashMap[String, Array[String]]
    val geneReader = Source.fromFile(infoPath + File.separator + "Ensembl_v65.Gencode_v10.ENSG.gene_info")
    for (line <- geneReader.getLines) {
      val lineSplit = line.replaceAll("\t{2,}", "\t").split("\t")
      geneMap = geneMap + ((lineSplit(0), lineSplit))
    }
    geneReader.close()

    //configure csv parser to tsv parser
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
    }

    //generate a new gdm file for each epigenome/column
    val fileNameSplit = fileName.split("_")
    val eid = fileNameSplit(0)
    val coreSplit = fileNameSplit(1).split("\\.")
    val NSource = if (fileName.contains("exon") || fileName.contains("exn"))
      "57epigenomes." + coreSplit(0) +".N."+fileNameSplit(1).split("\\.").slice(1, coreSplit.length-1).mkString(".")
    else
      "57epigenomes.N." + fileNameSplit(1).substring(0, fileNameSplit(1).lastIndexOf("."))
    val RPKMSource = if (fileName.contains("exon") || fileName.contains("exn"))
      "57epigenomes." + coreSplit(0) +".RPKM."+fileNameSplit(1).split("\\.").slice(1, coreSplit.length-1).mkString(".")
    else
      "57epigenomes.RPKM." + fileNameSplit(1).substring(0, fileNameSplit(1).lastIndexOf("."))

    //get the iterator over the tabular files
    val exprReaderN = CSVReader.open(new File(filePath + File.separator + NSource))
    val geneExpressionsN = exprReaderN.iteratorWithHeaders
    val exprReaderRPKM = CSVReader.open(new File(filePath + File.separator + RPKMSource))
    val geneExpressionsRPMK = exprReaderRPKM.iteratorWithHeaders

    //write the new lines on the gdm file
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath + File.separator + fileName)))) {
      writer => {
        //write the first five attributes/columns
        for (gene <- geneExpressionsN) {
          val geneInfo = geneMap(gene("gene_id"))
          val regCoo = if (gene.contains("exon_location")) {
            val exnLoc = gene("exon_location")
            val chr = exnLoc.split(":")(0)
            val start = exnLoc.split(":")(1).split("<")(0).split("-")(0)
            val stop = exnLoc.split(":")(1).split("<")(0).split("-")(1)
            val strnd = exnLoc.split(":")(1).split("<")(1) match {
              case "1" => "+"
              case "-1" => "-"
              case _ => "."
            }
            chr + "\t" + start + "\t" + stop + "\t" + strnd
          }
          else {
            val strnd = geneInfo(4) match {
              case "1" => "+"
              case "-1" => "-"
              case _ => "."
            }
            "chr" + geneInfo(1) + "\t" + geneInfo(2) + "\t" + geneInfo(3) + "\t" + strnd
          }
          if (geneInfo(geneInfo.length-1) == "NA")
            writer.write(s"$regCoo\t${gene(eid)}\t${geneExpressionsRPMK.next()(eid)}\t${gene("gene_id")}\t${geneInfo(5)}\t${geneInfo(6)}\tNA\tNA\tNA\n")
          else {
            val geneName = geneInfo(geneInfo.length-1).split("_\\[")(0)
            val geneSource = geneInfo(geneInfo.length-1).split("_\\[")(1).split(";")(0).split(":")(1)
            val geneAccession = geneInfo(geneInfo.length-1).split("_\\[")(1).split(";")(1).split(":")(1).dropRight(1)
            writer.write(s"$regCoo\t${gene(eid)}\t${geneExpressionsRPMK.next()(eid)}\t${gene("gene_id")}\t${geneInfo(5)}\t${geneInfo(6)}\t$geneName\t$geneSource\t$geneAccession\n")
          }
        }
      }
    }
    exprReaderN.close
    exprReaderN.close
  }

  /**
    * modify the HOTSPOT generated files so they match the same schema
    *
    * @param filename              name of the file
    * @param destinationPath       path in which save the modified file
    */
  def hotspotAdjustment(filename: String, destinationPath: String): Unit = {
    val testBroad = """.*\.broad.bed""".r
    val testAllPeaks = """.*\.all.peaks.bed""".r
    val unfixedFile = new File(destinationPath + File.separator + filename)
    val reader = Source.fromFile(unfixedFile)
    val newLines = new ListBuffer[String]
    for (line <- reader.getLines) {
      filename match {
        case testAllPeaks() =>
          val lineSplit = line.split("\t")
          lineSplit(4) = lineSplit(4).split("\\.")(0)
          newLines += (lineSplit.mkString("\t") + "\tNULL")
        case testBroad() =>
          val lineSplit = line.split("\t")
          newLines += (lineSplit.toList.take(4).mkString("\t") + "\tNULL\t" + lineSplit(4))
      }
    }
    reader.close()
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(unfixedFile)))) {
      writer => { newLines.foreach( newLine => writer.write(newLine + "\n"))
      }
    }
  }

  /**
    * add the stand column and remove the duplicated score column
    *
    * @param filename              name of the file
    * @param destinationPath       location where the modified file is saved
    */
  def DMRAdjustment(filename: String, destinationPath: String): Unit = {
    val unfixedFile = new File(destinationPath + File.separator + filename)
    val reader = Source.fromFile(unfixedFile)
    val newLines = new ListBuffer[String]
    for (line <- reader.getLines) {
      val lineSplit = line.split("\t")
      lineSplit(3) = "."
      newLines += lineSplit.mkString("\t")
    }
    reader.close()
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(unfixedFile)))) {
      writer => { newLines.foreach( newLine => writer.write(newLine + "\n"))
      }
    }
  }

  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    *
    * @param filename original filename
    * @param dataset  dataset where the file belongs to
    * @param source   source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset: GMQLDataset, source: GMQLSource): List[String] = {
    filename match {
      case patternRNAexpArch() =>
        if (filename.contains("N")) {
          val newNames = new ListBuffer[String]
          val outPath = source.outputFolder + File.separator +dataset.outputFolder + File.separator + "Transformations"
          val inputPath = source.outputFolder + File.separator +dataset.outputFolder + File.separator + "Downloads"
          val unzippedTabName = filename.substring(0, filename.lastIndexOf("."))
          if (Unzipper.unGzipIt(inputPath + File.separator + filename, outPath + File.separator + unzippedTabName)) {
            logger.info("unGzipping: " + filename + " DONE")
            //get the list of epigenomeIDs from the tabular file
            val headerReader = Source.fromFile(outPath + File.separator + unzippedTabName)
            val eidList: Array[String] = headerReader.getLines.take(1).toList.head.split("\t").filter(elem => elem.matches("E[0-9][0-9][0-9]"))
            headerReader.close()
            //parse the name of the new file from the old one
            eidList.foreach(eid => {
              val dataNewFileName = eid + "_" + unzippedTabName.replaceAll("RPKM.|N.|57epigenomes.", "") + ".bed"
              //val newFile = new File(outPath + File.separator + newDataFileName)
              val metadataNewFileName = dataNewFileName + ".meta"
              newNames += dataNewFileName
              newNames += metadataNewFileName
            })
          }

          else
            logger.warn("unGzipping: " + filename + " FAILED")
          if (Unzipper.unGzipIt(inputPath + File.separator + filename, outPath + File.separator + unzippedTabName.replace("N", "RPKM")))
            logger.info("unGzipping: " + filename + " DONE")
          else
            logger.warn("unGzipping: " + filename + "FAILED")

          newNames.toList
        }
        else
          List()
      case _ =>
        val dataFileNewName = filename.substring(0, filename.lastIndexOf(".")).replace("-", "_")
        val metadataNewFileName = dataFileNewName + ".meta"
        List[String](dataFileNewName, metadataNewFileName)
    }
  }
}