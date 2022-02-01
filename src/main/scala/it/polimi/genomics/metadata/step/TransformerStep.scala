package it.polimi.genomics.metadata.step

import java.io._

import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.SchemaFinder
import it.polimi.genomics.metadata.step.utils.DatasetNameUtil
import it.polimi.genomics.metadata.step.xml.Dataset
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.xml.XML

/**
  * Created by Nacho on 12/6/16.
  */
object TransformerStep extends Step {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Transforms the data and metadata into GDM friendly formats using the transformers.
    * Normalizes the metadata key names as indicated in the settings.
    * Puts the schema file into the transformations folders.
    *
    * @param source            source to be integrated.
    * @param parallelExecution defines if the execution is in parallel or sequential
    */
  override def execute(source: xml.Source, parallelExecution: Boolean): Unit = {
    if (source.transformEnabled) {

      logger.info("Starting integration for: " + source.outputFolder)
      val sourceId = FileDatabase.sourceId(source.name)

      //I load the renaming list from the source parameters.
      val metadataRenaming: Seq[(Regex, String)] =
        try {
          (XML.loadFile(source.rootOutputFolder + File.separator + source.parameters
            .filter(_._1.equalsIgnoreCase("metadata_replacement")).head._2) \\ "metadata_replace_list" \ "metadata_replace")
            .map(replacement => ((replacement \ "regex").text.r, (replacement \ "replace").text))
        }
        catch {
          case _: IOException =>
            logger.error("not valid metadata replacement xml file: ")
            Seq.empty
          case _: NoSuchElementException =>
            logger.info("no metadata replacement defined for: " + source.name)
            Seq.empty
        }
      //counters
      var modifiedRegionFilesSource = 0
      var modifiedMetadataFilesSource = 0
      var wrongSchemaFilesSource = 0
      //integration process for each dataset contained in the source.
      val integrateThreads = source.datasets.map((dataset: Dataset) => {
        new Thread {
          override def run(): Unit = {
            if (dataset.transformEnabled) {


              val transformationClass = Class
                .forName(source.transformer)
                .newInstance.asInstanceOf[Transformer]

              val t0Dataset: Long = System.nanoTime()
              var modifiedRegionFilesDataset = 0
              var modifiedMetadataFilesDataset = 0
              var wrongSchemaFilesDataset = 0
              var totalTransformedFiles = 0
              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)

              val datasetOutputFolder = dataset.fullDatasetOutputFolder
              val downloadsFolder = datasetOutputFolder + File.separator + "Downloads"
              val transformationsFolder = datasetOutputFolder + File.separator + "Transformations"


              val folder = new File(transformationsFolder)
              if (folder.exists()) {
                deleteFolder(folder)
              }


              logger.info("Starting download for: " + dataset.name)
              // puts the schema into the transformations folder.
              if (SchemaFinder.downloadSchema(source.rootOutputFolder, dataset, transformationsFolder, source))
                logger.debug("Schema downloaded for: " + dataset.name)
              else
                logger.warn("Schema not found for: " + dataset.name)


              if (!folder.exists()) {
                folder.mkdirs()
                logger.debug("Folder created: " + folder)
              }
              logger.info("Transformation for dataset: " + dataset.name)

              FileDatabase.delete(datasetId, Stage.TRANSFORM)
              // detect many-to-many type of transformation.
              val isManyToManyTransformation = dataset.getParameter("many_to_many_transform").getOrElse("false").toBoolean
              //id, filename, copy number.
              val candidates = {
                val tempCandidates: List[((String, Int), (Int, String, Int))] = FileDatabase.getFilesToProcess(datasetId, Stage.DOWNLOAD).toList.flatMap { file =>
                  val originalFileName =
                    if (file._3 == 1) file._2
                    else file._2.replaceFirst("\\.", "_" + file._3 + ".")

                  val fileDownloadPath = downloadsFolder + File.separator + originalFileName
                  val candidates = transformationClass.getCandidateNames(originalFileName, dataset, source)

                  logger.info(s"candidates: $originalFileName, $candidates")
                  val files = candidates.map(candidateName => {
                    (candidateName, FileDatabase.fileId(datasetId, fileDownloadPath, Stage.TRANSFORM, candidateName, !isManyToManyTransformation))
                  })

                  files.map((_, file))
                }

                // select the candidates which has corresponding candidate (region-> meta or meta->region
                val candidateNameSet = tempCandidates.map(_._1._1).toSet
                tempCandidates.filter { case ((candidateName, _), _) =>
                  if (candidateName.endsWith(".meta"))
                    candidateNameSet.contains(candidateName.substring(0, candidateName.length - 5))
                  else
                    candidateNameSet.contains(candidateName + ".meta")
                }
              }.sortBy(_._1._1)

              // candidates.foreach(t => println(t._1._1))

              logger.info("-------------------------")
              // candidates.foreach(t => logger.info(s"all candidates: $t"))


              val filesToTransform = candidates.length

              /**
               * Calls the source specific Transformer implementation.
               * @param candidateName name of the candidate file (output of the transformation).
               * @param fileId fileId of the candidate.
               * @param file a tuple (fileId, name, copy_number) of the origin file.
               */
              def transform(candidateName:String, fileId:Int, file:(Int, String, Int)):(Boolean, String) ={
                val originalFileName =
                  if (file._3 == 1) file._2
                  else file._2.replaceFirst("\\.", "_" + file._3 + ".")

                val fileNameAndCopyNumber = FileDatabase.getFileNameAndCopyNumber(fileId)
                val name =
                  if (fileNameAndCopyNumber._2 == 1) fileNameAndCopyNumber._1
                  else fileNameAndCopyNumber._1.replaceFirst("\\.", "_" + fileNameAndCopyNumber._2 + ".")
                val originDetails = FileDatabase.getFileDetails(file._1)

                //I always transform, so the boolean checkIfUpdate is not used here.
                FileDatabase.checkIfUpdateFile(fileId, originDetails._1, originDetails._2, originDetails._3)
                ( transformationClass.transform(source, downloadsFolder, transformationsFolder, originalFileName, name), name )
              }

              // a set to remember the fileId of the candidates already passed as input to the method postProcess
              val postProcessedFileIds:mutable.Set[Int] = mutable.HashSet.empty[Int]
              /**
               * Region files are checked against the schema and the regions are sorted within each file. Metadata files
               * are enriched with manually curated attributes. At the end of this method, each candidate file is marked
               * as UPDATED or FAILED.
               * If the transformation taking place is of kind many-to-many files and the candidate is a region file, the
               * post-processing occurs only once for each region output file.
               * @param isTransformed a boolean indicating whether the candidate file has been generated.
               * @param name the name of the file generated for this candidate.
               * @param candidateName the candidate name.
               * @param fileId the fileId of the candidate.
               * @param file a tuple (fileId, name, copy_number) of the origin file.
               */
              def postProcess(isTransformed: Boolean, name: String, candidateName: String, fileId: Int, file: (Int, String, Int)):Unit = {
                if(isTransformed) {
                  val fileTransformationPath = transformationsFolder + File.separator + name
                  if (name.endsWith(".meta")) {
                    val separator =
                      if (dataset.source.parameters.exists(_._1 == "metadata_name_separation_char"))
                        dataset.source.parameters.filter(_._1 == "metadata_name_separation_char").head._2
                      else
                        "__"


                    // enrich metadata file
                    {
                      val regionFileId = Option(candidates.collect {
                        case ((candidateNameTemp, _), (originFileID, _, _))
                          if candidateName.substring(0, candidateName.length - 5) == candidateNameTemp => originFileID
                      }).getOrElse(List(file._1))

                      val filesDetailsOptions = regionFileId.map(FileDatabase.getFileAllDetails)


                      val (fileSize, md5) = {
                        val regionFileName = fileTransformationPath.substring(0, fileTransformationPath.length - 5)
                        println("regionFileName: [" + regionFileName + "]")
                        val file = new File(regionFileName)
                        val fileSize = file.length()
                        val fis = new FileInputStream(file)
                        val md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis)
                        fis.close()
                        (fileSize, md5)
                      }

                      val writer = new FileWriter(fileTransformationPath, true)
                      writer.write("manually_curated" + separator + "local_file_size\t" + fileSize + "\n")
                      writer.write("manually_curated" + separator + "local_md5\t" + md5 + "\n")


                      filesDetailsOptions.filter(_.isDefined).foreach { fileDetailsOption =>
                        val fileDetails = fileDetailsOption.get
                        if (fileDetails.lastUpdate.nonEmpty)
                          writer.write("manually_curated" + separator + "download_date\t" + fileDetails.lastUpdate + "\n")
                        if (fileDetails.hash.nonEmpty)
                          writer.write("manually_curated" + separator + "origin_md5\t" + fileDetails.hash + "\n")
                        if (fileDetails.originLastUpdate.nonEmpty)
                          writer.write("manually_curated" + separator + "origin_last_modified_date\t" + fileDetails.originLastUpdate + "\n")
                        if (fileDetails.originSize.nonEmpty)
                          writer.write("manually_curated" + separator + "origin_file_size\t" + fileDetails.originSize + "\n")
                      }


                      val maxCopy = FileDatabase.getMaxCopyNumber(datasetId, file._2, Stage.DOWNLOAD)
                      if (maxCopy > 1) {
                        writer.write("manually_curated" + separator + "file_copy_total_count\t" + maxCopy + "\n")
                        writer.write("manually_curated" + separator + "file_copy_number\t" + file._3 + "\n")
                        writer.write("manually_curated" + separator + "file_name_replaced\ttrue\n")
                      }
                      writer.close()
                    }


                    //metadata renaming. (standardizing of the metadata values should happen here also.
                    if ( /*!metadataRenaming.isEmpty &&*/ changeMetadataKeys(metadataRenaming, fileTransformationPath))
                      modifiedMetadataFilesDataset = modifiedMetadataFilesDataset + 1
                    if(!isManyToManyTransformation || postProcessedFileIds.add(fileId))
                      totalTransformedFiles = totalTransformedFiles + 1
                    FileDatabase.markAsUpdated(fileId, new File(fileTransformationPath).length.toString)
                  }
                  //if not meta, is region data.modifiedMetadataFilesDataset+modifiedRegionFilesDataset
                  else {
                    if(!isManyToManyTransformation || postProcessedFileIds.add(fileId)) {
                    /*val dataUrl = FileDatabase.getFileDetails(fileId)._4*/
                    /*val metaUrl = if (dataset.parameters.exists(_._1 == "spreadsheet_url"))
                    dataset.parameters.filter(_._1 == "region_sorting").head._2
                  else ""
                  val fw = new FileWriter(fileTransformationPath + ".meta", true)
                  try {
                    fw.write(metaUrl)
                  }
                  finally fw.close()*/
                      val schemaFilePath = transformationsFolder + File.separator + dataset.name + ".schema"
                      val modifiedAndSchema = checkRegionData(fileTransformationPath, schemaFilePath)
                      if (modifiedAndSchema._1)
                        modifiedRegionFilesDataset = modifiedRegionFilesDataset + 1
                      if (!modifiedAndSchema._2)
                        wrongSchemaFilesDataset = wrongSchemaFilesDataset + 1
                      if (!dataset.parameters.exists(_._1 == "region_sorting") || dataset.parameters.filter(_._1 == "region_sorting").head._2 == "true")
                        if (Try(regionFileSort(fileTransformationPath)).isFailure)
                          logger.warn(s"fail to sort $fileTransformationPath")
                        else
                          logger.debug(s"$fileTransformationPath successfully sorted")
                      FileDatabase.markAsUpdated(fileId, new File(fileTransformationPath).length.toString)
                      totalTransformedFiles = totalTransformedFiles + 1
                    }
                    //standardization of the region data should be here.
                  }
                }
                else
                  FileDatabase.markAsFailed(fileId)
              }

              transformationClass.onBeforeTransformation(dataset)
              // transform files
              val transformedWithNames = candidates.map { case ((candidateName, fileId), file) =>

                val result = transform(candidateName, fileId, file)

                if(!isManyToManyTransformation)
                  postProcess(result._1, result._2, candidateName, fileId, file)
                result
              }
              transformationClass.onAllTransformationsDone(dataset, isManyToManyTransformation)

              // run delayed postprocessing
              if(isManyToManyTransformation) {
                transformedWithNames zip candidates foreach { case ((isTransformed, name), ((candidateName,  fileId), file)) =>
                  postProcess(isTransformed, name, candidateName, fileId, file)
                }
              }

              FileDatabase.markAsOutdated(datasetId, Stage.TRANSFORM)
              //          FileDatabase.markAsProcessed(datasetId, STAGE.DOWNLOAD)
              FileDatabase.runDatasetTransformAppend(datasetId, dataset, filesToTransform, totalTransformedFiles)
              modifiedMetadataFilesSource = modifiedMetadataFilesSource + modifiedMetadataFilesDataset
              modifiedRegionFilesSource = modifiedRegionFilesSource + modifiedRegionFilesDataset
              wrongSchemaFilesSource = wrongSchemaFilesSource + wrongSchemaFilesDataset
              logger.info(modifiedRegionFilesDataset + " region data files modified in dataset: " + dataset.name)
              logger.info(modifiedMetadataFilesDataset + " metadata files modified in dataset: " + dataset.name)
              logger.info(wrongSchemaFilesDataset + " region data files do not respect the schema in dataset: " + dataset.name)
              val t1Dataset = System.nanoTime()
              logger.info(s"Total time for transformation dataset ${dataset.name}: ${getTotalTimeFormatted(t0Dataset, t1Dataset)}")

              DatasetNameUtil.saveDatasetName(dataset)

            }
          }
        }
      })
      if (parallelExecution) {
        integrateThreads.foreach(_.start())
        integrateThreads.foreach(_.join())
      }
      else {
        for (thread <- integrateThreads) {
          thread.run()
          //          thread.join()
        }
      }
      logger.info(modifiedRegionFilesSource + " region data files modified in source: " + source.name)
      logger.info(modifiedMetadataFilesSource + " metadata files modified in source: " + source.name)
      logger.info(wrongSchemaFilesSource + " region data files do not respect the schema in source: " + source.name)
      logger.info(s"Source ${source.name} transformation finished")
    }
  }


  /**
    * Checks the region data file has the columns corresponding to the schema, tries to parse the data
    * according to the schema types and normalizes values in the region data fields.
    *
    * @param dataFilePath   path of the region data file.
    * @param schemaFilePath path of the schema file.
    * @return if the file was correctly modified and if it fits the schema.
    */
  def checkRegionData(dataFilePath: String, schemaFilePath: String): (Boolean, Boolean) = {
    //first I read the schema.
    if (new File(schemaFilePath).exists()) {
      val continue = {
        try {
          (XML.loadFile(schemaFilePath) \\ "field").nonEmpty
          true
        }
        catch {
          case _: Exception => false
        }
      }
      if (continue) {
        //load the schema in memory
        val fields: Seq[(String, String)] = (XML.loadFile(schemaFilePath) \\ "field").map(field => (field.text, (field \ "@type").text))

        val nodeSeq = XML.loadFile(schemaFilePath) \\ "gmqlSchema" \ "@type"
        //detect schema type
        val schemaType = if (nodeSeq.isEmpty)
          "tab"
        else if (nodeSeq.head.toString().toLowerCase.matches("tab|gtf"))
          nodeSeq.head.toString().toLowerCase
        else {
          logger.warn(s"$schemaFilePath gmqlSchema type unknown, trying to treat it as a TAB")
          "tab"
        }
        //generate a temp file
        val tempFile = dataFilePath + ".temp"
        val writer = new PrintWriter(tempFile)

        //event register
        val missingValueCount = Array.fill(fields.size)(0)
        val typeMismatchCount = Array.fill(fields.size)(0)
        var wrongAttNumCount = 0
        var strandBadValCount = 0
        var chromBadValCount = 0
        var gtfMandatoryMissing = 0
        var gtfOptionalMissing = 0
        val gtfOptionalWrongName = Array.fill(fields.size)(0)
        val gtfOptionalWrongFormt = Array.fill(fields.size)(0)
        //only replaces if everything goes right, if there is an error, we do not replace.
        var correctSchema = true
        var modified = false

        //scanning the file to check the consistency of each row to schema and manage missing value
        val reader = Source.fromFile(dataFilePath)
        reader.getLines().foreach(f = line => {

          var writeLine = ""
          var isRemoved = false
          val splitLine = line.split("\t", -1)
          val regAttributes = if (schemaType == "gtf") {
            //check if there are all the mandatory attribute
            val optionalValues = ListBuffer[String]()
            if (splitLine.length != 9) {
              gtfMandatoryMissing += 1
              isRemoved = true
            }
            else {
              //check if the last mandatory attribute containing the optional attributes is in the correct format
              val optionalAttributes = splitLine(8).split("; |;")
              //check if there are all the optional attribute
              if (optionalAttributes.length + splitLine.length - 1 != fields.length) {
                gtfOptionalMissing += 1
                isRemoved = true
              }
              optionalAttributes.foreach(optAttribute => {
                //check the format of optional attributes and extract the value
                if (optAttribute.matches(""".+ ".*"""")) {
                  val optAttributeSplit = optAttribute.split(" ")
                  //check the optional attribute name
                  if (optAttributeSplit(0) != fields(optionalAttributes.indexOf(optAttribute) + 8)._1)
                    gtfOptionalWrongName(optionalAttributes.indexOf(optAttribute) + 8) += 1
                  optionalValues += optAttributeSplit(1).dropRight(1).drop(1)
                }
                else if (optAttribute.matches(""".+ (^["].*^["]|^["])?""")) {
                  val optAttributeSplit = optAttribute.split(" ")
                  if (optAttributeSplit(0) != fields(optionalAttributes.indexOf(optAttribute) + 8)._1)
                    gtfOptionalWrongName(optionalAttributes.indexOf(optAttribute) + 8) += 1
                  if (optAttributeSplit.length > 1)
                    optionalValues += optAttributeSplit(1)
                  else
                    optionalValues += ""
                }
                else {
                  gtfOptionalWrongFormt(optionalValues.indexOf(optAttribute) + 8) += 1
                  optionalValues += ""
                }
              })
            }
            splitLine.dropRight(1) ++ optionalValues
          }
          else {
            if (splitLine.length != fields.length) {
              wrongAttNumCount += 1
              isRemoved = true
            }
            splitLine
          }
          if (!isRemoved) { //check if number of region attributes is consistent to schema
            for (i <- 0 until regAttributes.size) {
              //managing missing value
              if (regAttributes(i) == "NR" || regAttributes(i) == "" || regAttributes(i).toUpperCase == "NULL" || regAttributes(i).toUpperCase == "N/A" ||
                regAttributes(i).toUpperCase == "NA" || (regAttributes(i) == "." && fields(i)._1 == "score")) {
                missingValueCount(i) += 1
                val oldValue = regAttributes(i)
                //some attribute must be treated in different way if missing
                if (fields(i)._1.toLowerCase == "score" || (schemaType == "gtf" && i < 8))
                  regAttributes(i) = "."
                else if (fields(i)._2.toUpperCase == "STRING" || fields(i)._2.toUpperCase == "CHAR")
                  regAttributes(i) = ""
                else
                  regAttributes(i) = "NULL"

                //check if the missing value has been modified
                if (oldValue != regAttributes(i))
                  modified = true
              }
              else {
                //type consistency check
                val typeMatch = fields(i)._2.toUpperCase match {
                  case "LONG" => Try {
                    regAttributes(i).toLong
                  }
                  case "DOUBLE" => Try {
                    regAttributes(i).toDouble
                  }
                  case "INTEGER" => Try {
                    regAttributes(i).toInt
                  }
                  case "CHAR" => if (regAttributes(i).length == 1) Success(regAttributes(i)) else Failure(new Exception("Not a char"))
                  case "STRING" => Success(regAttributes(i))
                  case _ => Failure(new Exception("Region attribute invalid type in schema"))
                }

                typeMatch match {
                  case Success(_) => //specific attribute value check
                    //strand value check
                    val strandAttributeNames: List[String] = List("strand")
                    if (strandAttributeNames.contains(fields(i)._1.toLowerCase))
                      regAttributes(i) match {
                        case "+" | "-" | "*" | "." =>
                        case _ =>
                          regAttributes(i) = "."
                          strandBadValCount += 1
                          modified = true
                      }
                    //chromosome value check
                    val chromAttributeNames: List[String] = List("seqname", "seqnames", "chr", "chrom", "chromosome")
                    if (chromAttributeNames.contains(fields(i)._1.toLowerCase)) {
                      val validChrom = "chr[0-9A-Z]{1,2}".r
                      val invalidChrom1 = "[0-9A-Z]{1,2}".r
                      val invalidChrom2 = "[cC][hH][rR][0-9A-Za-z]{1,2}".r
                      regAttributes(i) match {
                        case validChrom() =>
                        case invalidChrom1() =>
                          regAttributes(i) = "chr" + regAttributes(i)
                          modified = true
                        case invalidChrom2() => regAttributes(i) = "chr" + regAttributes(i).drop(3).toUpperCase
                          modified = true
                        case _ => chromBadValCount += 1
                      }
                    }
                  case Failure(_) => //action to perform in case of type mismatch
                    typeMismatchCount(i) += 1
                    correctSchema = false
                }
              }

              //write region on temp file
              writeLine = writeLine + (if (i == 0) regAttributes(i)
              else if (i >= 8 && schemaType == "gtf")
                if (i == 8) s"""\t${fields(i)._1} "${regAttributes(i)}"""" else s"""; ${fields(i)._1} "${regAttributes(i)}""""
              else
                "\t" + regAttributes(i))
            }
            writeLine = writeLine + "\n"
            writer.write(writeLine)
          }
          else {
            modified = true //schema still correct because the wrong line is removed.
          }
        })
        reader.close()
        writer.close()
        //if there are changes to the lines, should replace the file.
        if (modified) {
          //replace the file, and remove the temporary one.
          try {
            val df = new File(dataFilePath)
            val tf = new File(tempFile)
            df.delete
            tf.renameTo(df)
          }
          catch {
            case _: IOException => logger.warn("could not change the file " + dataFilePath)
          }
        }
        else {
          new File(tempFile).delete()
        }

        if (gtfMandatoryMissing > 0)
          logger.info(s"In $dataFilePath: $gtfMandatoryMissing lines with wrong numbers of mandatory attributes removed.")
        if (gtfOptionalMissing > 0)
          logger.info(s"In $dataFilePath: $gtfOptionalMissing lines with wrong numbers of optional attributes removed.")
        if (wrongAttNumCount > 0)
          logger.info(s"In $dataFilePath: $wrongAttNumCount lines with wrong numbers of attributes removed.")
        for (i <- fields.indices)
          if (missingValueCount(i) > 0)
            logger.info(s"In $dataFilePath attribute ${fields(i)._1}: ${missingValueCount(i)} missing value.")
        for (i <- fields.indices)
          if (typeMismatchCount(i) > 0)
            logger.info(s"In $dataFilePath attribute ${fields(i)._1}: ${typeMismatchCount(i)} type mismatch.")
        for (i <- fields.indices)
          if (gtfOptionalWrongFormt(i) > 0)
            logger.info(s"In $dataFilePath optional attribute ${fields(i)._1}: ${gtfOptionalWrongFormt(i)} wrong optional attribute format.")
        for (i <- fields.indices)
          if (gtfOptionalWrongName(i) > 0)
            logger.info(s"In $dataFilePath attribute ${fields(i)._1}: ${gtfOptionalWrongName(i)} wrong optional attribute name.")
        if (strandBadValCount > 0)
          logger.info(s"In $dataFilePath: $strandBadValCount lines with invalid strand value replaced with default value.")
        if (chromBadValCount > 0)
          logger.info(s"In $dataFilePath: $chromBadValCount lines with invalid chrom value.")
        (modified, correctSchema)
      }
      else
        (false, false)
    }
    else
      (false, false)
    (true, true)
  }

  /**
    * changes the name of key attribute in metadata.
    * replaces all parts of the key value that matches a regular expression.
    *
    * @param changeKeys       pair regex, replacement. uses regex.replaceAll(_1,_2)
    * @param metadataFilePath origin file of metadata.
    */
  def changeMetadataKeys(changeKeys: Seq[(Regex, String)], metadataFilePath: String): Boolean = {
    var replaced = false
    if (new File(metadataFilePath).exists()) {
      var metadataList: Seq[(String, String)] = Seq[(String, String)]()
      val reader = Source.fromFile(metadataFilePath)
      reader.getLines().foreach(line => {
        val split = line.split("\t", -1)
        if (split.length == 2) {
          var metadataKey = split(0)
          val metadataValue = split(1)
          changeKeys.filter(change => change._1.findFirstIn(metadataKey).isDefined).foreach(change => {
            replaced = true
            metadataKey = change._1.replaceFirstIn(metadataKey, change._2)
          })
          //this is already handled in metadataReplacementTcga.xml
          if (metadataKey.contains(" ")) {
            logger.debug(s"$metadataKey replaced to ${metadataKey.replace(" ", "_")}")
            metadataKey = metadataKey.replace(" ", "_")
          }
          if (metadataKey.contains("|")) {
            logger.debug(s"$metadataKey replaced to ${metadataKey.replace("|", "__")}")
            metadataKey = metadataKey.replace("|", "__")
          }
          //Ensure metadata keys are in lowercase perche vogliamo cosi -- mm
          metadataKey = metadataKey.toLowerCase
          if (!isValidJavaIdentifier(metadataKey)) {
            logger.debug(s"$metadataKey replaced to ${transformToValidJavaIdentifier(metadataKey)}")
            metadataKey = transformToValidJavaIdentifier(metadataKey)
          }
          if (!metadataList.contains((metadataKey, metadataValue))) {
            metadataList = metadataList :+ (metadataKey, metadataValue)
          }
        }
        else {
          logger.warn("file: " + metadataFilePath + " should have 2 columns. Check this line that was excluded: " + line)
        }
      })
      reader.close()

      val tempFile = metadataFilePath + ".temp"
      val writer = new PrintWriter(tempFile)
      metadataList.sortWith((A, B) => A._1.compare(B._1) < 0).foreach(metadata => {
        writer.write(metadata._1 + "\t" + metadata._2 + "\n")
      })
      writer.close()

      try {
        val df = new File(metadataFilePath)
        val tf = new File(tempFile)
        df.delete
        tf.renameTo(df)
      }
      catch {
        case _: IOException => logger.error("could not change metadata key on the file " + metadataFilePath)
      }
    }
    replaced
  }


  /**
    * gets the time between 2 timestamps in hh:mm:ss format
    *
    * @param t0 start time
    * @param t1 end time
    * @return hh:mm:ss as string
    */
  def getTotalTimeFormatted(t0: Long, t1: Long): String = {

    val hours = Integer.parseInt("" + (t1 - t0) / 1000000000 / 60 / 60)
    val minutes = Integer.parseInt("" + ((t1 - t0) / 1000000000 / 60 - hours * 60))
    val seconds = Integer.parseInt("" + ((t1 - t0) / 1000000000 - hours * 60 * 60 - minutes * 60))
    s"$hours:$minutes:$seconds"
  }

  /**
    * Traverses a string checking all characters are valid to be java identifiers.
    *
    * @param s candidate name for identifier
    * @return whethers the candidate name is valid or not.
    */
  def isValidJavaIdentifier(s: String): Boolean = {
    var isValid = true
    if (s == "")
      isValid = false
    else if (!Character.isJavaIdentifierStart(s.charAt(0)))
      isValid = false

    for (i <- 1 until s.length)
      if (!Character.isJavaIdentifierPart(s.charAt(i))) {
        isValid = false
      }
    isValid
  }

  /**
    * makes sure the name of a metadata attribute is valid for java instantiation
    *
    * @param s candidate name for attribute.
    * @return name corrected if needed.
    */
  def transformToValidJavaIdentifier(s: String): String = {
    var output = s.map { c => if (Character.isJavaIdentifierPart(c)) c else '_' }
    if (!Character.isJavaIdentifierStart(output.head))
      output = output.replaceFirst(output.head.toString, "_")
    output
  }

  /**
    * deletes folder recursively
    *
    * @param path base folder path
    */
  //TODO move to a util object, used in many steps
  def deleteFolder(path: File): Unit = {
    try {
      if (path.exists()) {
        try {
          val files = path.listFiles()
          files.foreach(file => {
            try {
              if (file.isDirectory)
                deleteFolder(file)
              else
                try {
                  file.delete()
                }
                catch {
                  case ex: SecurityException => logger.warn(s"Couldn't delete $path: ${ex.getMessage}")
                }
            }
            catch {
              case ex: SecurityException => logger.warn(s"Couldn't access $path: ${ex.getMessage}")
            }
          })
        }
        catch {
          case ex: SecurityException => logger.warn(s"Couldn't list files from $path: ${ex.getMessage}")
        }
      }
    }
    catch {
      case ex: SecurityException => logger.warn(s"Couldn't delete folder $path: ${ex.getMessage}")
    }
  }

  /**
    * sort region by chrom, start and stop coordinates
    *
    * @param filePath file to sort
    */
  def regionFileSort(filePath: String): Unit = {
    var tempMap: TreeMap[(String, Long, Long), mutable.Queue[String]] = new TreeMap[(String, Long, Long), mutable.Queue[String]]
    val reader = Source.fromFile(filePath)
    for (line <- reader.getLines) {
      val lineSplit = line.split("\t")
      val regionID = (lineSplit(0), lineSplit(1).toLong, lineSplit(2).toLong)
      if (tempMap.contains(regionID))
        tempMap(regionID) += line
      else
        tempMap = tempMap + ((regionID, mutable.Queue(line)))
    }
    reader.close()
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(filePath))))) {
      writer => {
        tempMap.foreach(pair =>
          while (pair._2.nonEmpty) writer.write(pair._2.dequeue() + "\n"))
      }
    }
  }

  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try {
      block(resource)
    }
    finally {
      resource.close()
    }
  }
}
