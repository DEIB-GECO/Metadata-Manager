package it.polimi.genomics.importer.GMQLImporter

import java.io.{File, FileWriter, IOException, PrintWriter}

import it.polimi.genomics.importer.DefaultImporter.schemaFinder
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.xml.XML

/**
  * Created by nachon on 12/6/16.
  */
object Integrator {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Transforms the data and metadata into GDM friendly formats using the transformers.
    * Normalizes the metadata key names and values.
    * Normalizes the region data values.
    * Puts the schema file into the transformations folders.
    * @param source source to be integrated.
    */
  def integrate(source: GMQLSource): Unit = {
    if (source.transformEnabled) {
      logger.info("Starting integration for: " + source.outputFolder)
      val sourceId = FileDatabase.sourceId(source.name)

      //I load the renaming list from the source parameters.
      val metadataRenaming: Seq[(String, String)] =
        try {
          (XML.loadFile(source.rootOutputFolder + File.separator + source.parameters
            .filter(_._1.equalsIgnoreCase("metadata_replacement")).head._2) \\ "metadata_replace_list" \ "metadata_replace")
            .map(replacement => ((replacement \ "regex").text, (replacement \ "replace").text))
        }
        catch {
          case e: IOException =>
            logger.error("not valid metadata replacement xml file: ")
            Seq[(String, String)]()
          case e: NoSuchElementException =>
            logger.info("no metadata replacement defined for: " + source.name)
            Seq[(String, String)]()
        }
      //counters
      var modifiedRegionFilesSource = 0
      var modifiedMetadataFilesSource = 0
      var wrongSchemaFilesSource = 0
      //integration process for each dataset contained in the source.
      val integrateThreads = source.datasets.map(dataset => {
        new Thread {
          override def run(): Unit = {
            if (dataset.transformEnabled) {
              var modifiedRegionFilesDataset = 0
              var modifiedMetadataFilesDataset = 0
              var wrongSchemaFilesDataset = 0
              var totalTransformedFiles = 0
              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)

              val datasetOutputFolder = source.outputFolder + File.separator + dataset.outputFolder
              val downloadsFolder = datasetOutputFolder + File.separator + "Downloads"
              val transformationsFolder = datasetOutputFolder + File.separator + "Transformations"

              // puts the schema into the transformations folder.
              if (schemaFinder.downloadSchema(source.rootOutputFolder, dataset, transformationsFolder, source))
                logger.debug("Schema downloaded for: " + dataset.name)
              else
                logger.warn("Schema not found for: " + dataset.name)

              val folder = new File(transformationsFolder)
              if (!folder.exists()) {
                folder.mkdirs()
                logger.debug("Folder created: " + folder)
              }
              logger.info("Transformation for dataset: " + dataset.name)

              FileDatabase.markToCompare(datasetId, STAGE.TRANSFORM)
              //id, filename, copy number.
              var filesToTransform = 0
              FileDatabase.getFilesToProcess(datasetId, STAGE.DOWNLOAD).foreach(file => {
                val originalFileName =
                  if (file._3 == 1) file._2
                  else file._2.replaceFirst("\\.", "_" + file._3 + ".")

                val fileDownloadPath = downloadsFolder + File.separator + originalFileName
                val files = Class
                  .forName(source.transformer)
                  .newInstance.asInstanceOf[GMQLTransformer]
                  .getCandidateNames(originalFileName, dataset, source)
                  .map(candidateName => {
                    FileDatabase.fileId(datasetId, fileDownloadPath, STAGE.TRANSFORM, candidateName)
                  })
                filesToTransform = filesToTransform + files.length
                files.foreach(fileId => {
                  val fileNameAndCopyNumber = FileDatabase.getFileNameAndCopyNumber(fileId)
                  val name =
                    if (fileNameAndCopyNumber._2 == 1) fileNameAndCopyNumber._1
                    else fileNameAndCopyNumber._1.replaceFirst("\\.", "_" + fileNameAndCopyNumber._2 + ".")
                  val originDetails = FileDatabase.getFileDetails(file._1)

                  //I always transform, so the boolean checkIfUpdate is not used here.
                  FileDatabase.checkIfUpdateFile(fileId, originDetails._1, originDetails._2, originDetails._3)
                  val transformed = Class
                    .forName(source.transformer)
                    .newInstance.asInstanceOf[GMQLTransformer]
                    .transform(source, downloadsFolder, transformationsFolder, originalFileName, name)
                  val fileTransformationPath = transformationsFolder + File.separator + name
                  //add copy numbers if needed.
                  if(transformed) {
                    if (name.endsWith(".meta")) {
                      val separator =
                        if (source.parameters.exists(_._1 == "metadata_name_separation_char"))
                          source.parameters.filter(_._1 == "metadata_name_separation_char").head._2
                        else
                          "__"
                      val maxCopy = FileDatabase.getMaxCopyNumber(datasetId, file._2, STAGE.DOWNLOAD)
                      if (maxCopy > 1) {
                        val writer = new FileWriter(fileTransformationPath, true)
                        writer.write("manually_curated" + separator + "file_copy_total_count\t" + maxCopy + "\n")
                        writer.write("manually_curated" + separator + "file_copy_number\t" + file._3 + "\n")
                        writer.write("manually_curated" + separator + "file_name_replaced\ttrue\n")
                        writer.close()
                      }
                      //metadata renaming. (standardizing of the metadata values should happen here also.
                      if (changeMetadataKeys(metadataRenaming, fileTransformationPath))
                        modifiedMetadataFilesDataset = modifiedMetadataFilesDataset + 1
                      totalTransformedFiles = totalTransformedFiles + 1
                    }
                    //if not meta, is region data.modifiedMetadataFilesDataset+modifiedRegionFilesDataset
                    else {
                      val schemaFilePath = transformationsFolder + File.separator + dataset.name + ".schema"
                      val modifiedAndSchema = checkRegionData(fileTransformationPath, schemaFilePath)
                      if (modifiedAndSchema._1)
                        modifiedRegionFilesDataset = modifiedRegionFilesDataset + 1
                      if (!modifiedAndSchema._2)
                        wrongSchemaFilesDataset = wrongSchemaFilesDataset + 1
                      totalTransformedFiles = totalTransformedFiles + 1

                    }
                    //standardization of the region data should be here.
                    FileDatabase.markAsUpdated(fileId, new File(fileTransformationPath).length.toString)
                  }
                  else
                    FileDatabase.markAsFailed(fileId)
                  //                }
                })
              })
              FileDatabase.markAsOutdated(datasetId, STAGE.TRANSFORM)
              //          FileDatabase.markAsProcessed(datasetId, STAGE.DOWNLOAD)
              FileDatabase.runDatasetTransformAppend(datasetId, dataset, filesToTransform, totalTransformedFiles)
              modifiedMetadataFilesSource = modifiedMetadataFilesSource + modifiedMetadataFilesDataset
              modifiedRegionFilesSource = modifiedRegionFilesSource + modifiedRegionFilesDataset
              wrongSchemaFilesSource = wrongSchemaFilesSource + wrongSchemaFilesDataset
              logger.info(modifiedRegionFilesDataset + " region data files modified in dataset: " + dataset.name)
              logger.info(modifiedMetadataFilesDataset + " metadata files modified in dataset: " + dataset.name)
              logger.info(wrongSchemaFilesDataset + " region data files do not respect the schema in dataset: " + dataset.name)
            }
          }
        }
      })
      integrateThreads.foreach(_.start())
      integrateThreads.foreach(_.join())
      logger.info(modifiedRegionFilesSource + " region data files modified in source: " + source.name)
      logger.info(modifiedMetadataFilesSource + " metadata files modified in source: " + source.name)
      logger.info(wrongSchemaFilesSource + " region data files do not respect the schema in source: " + source.name)
    }
  }

  /**
    * Checks the region data file has the columns corresponding to the schema, tries to parse the data
    * according to the schema types and normalizes values in the region data fields.
    * @param dataFilePath path of the region data file.
    * @param schemaFilePath path of the schema file.
    * @return if the file was correctly modified and if it fits the schema.
    */
  def checkRegionData(dataFilePath: String, schemaFilePath: String): (Boolean,Boolean) ={
    //first I read the schema.
    if(new File(schemaFilePath).exists()) {
      val continue = {
        try {
          (XML.loadFile(schemaFilePath) \\ "field").nonEmpty
          true
        }
        catch {
          case e: Exception => false
        }
      }
      if (continue) {
        val fields = (XML.loadFile(schemaFilePath) \\ "field").map(field => (field.text, (field \ "@type").text))

        val tempFile = dataFilePath + ".temp"
        val writer = new PrintWriter(tempFile)
        //only replaces if everything goes right, if there is an error, we do not replace.
        var correctSchema = true
        var replace = false
        Source.fromFile(dataFilePath).getLines().foreach(line => {
          val splitLine = line.split("\t")
          var writeLine = ""
          //here I have to check the schema
          if (splitLine.size == fields.size) {
            for (i <- 0 until splitLine.size) {

              //try to parse for the type fields(i)._2

              //if its a particular column, do particular action fields(i)._1
              writeLine = writeLine + (if (i == 0) splitLine(i) else "\t" + splitLine(i))
            }
            writeLine = writeLine + "\n"
            writer.write(writeLine)
            //if there are changes to the lines, should replace the file.
            replace = true
          }
          else {
            if (correctSchema)
              logger.warn("file: " + dataFilePath + " should have " + fields.length +
                " columns. Instead has " + line.split("\t").length)
            correctSchema = false
          }
        })
        writer.close()
        if (replace) {
          //replace the file, and remove the temporary one.
          try {
            import java.io.{File, FileInputStream, FileOutputStream}
            val src = new File(tempFile)
            val dest = new File(dataFilePath)
            new FileOutputStream(dest) getChannel() transferFrom(
              new FileInputStream(src) getChannel, 0, Long.MaxValue)
            new File(tempFile).delete()
          }
          catch {
            case e: IOException => logger.error("could not change the file " + dataFilePath)
          }
        }
        else {
          new File(tempFile).delete()
        }
        (replace, correctSchema)
      }
      else
        (false,false)
    }
    else
      (false,false)
  }
  /**
    * changes the name of key attribute in metadata.
    * replaces all parts of the key value that matches a regular expression.
    * @param changeKeys pair regex, replacement. uses regex.replaceAll(_1,_2)
    * @param metadataFilePath origin file of metadata.
    */
  def changeMetadataKeys(changeKeys: Seq[(String,String)], metadataFilePath: String): Boolean ={
    var replaced = false
    if(new File(metadataFilePath).exists()){
      var metadataList: Seq[(String, String)] = Seq[(String,String)]()
      Source.fromFile(metadataFilePath).getLines().foreach(line=>{
        if(line.split('\t').length==2) {
          var metadataKey = line.split('\t')(0)
          val metadataValue = line.split('\t')(1)
          changeKeys.filter(change => change._1.r.findFirstIn(metadataKey).isDefined).foreach(change =>{

            replaced = true
            metadataKey = change._1.r.replaceFirstIn(metadataKey, change._2)
          })
          //this is already handled in metadataReplacementTcga.xml
          metadataKey = metadataKey.replace(" ", "_").replace("|","__")
          if(!metadataList.contains((metadataKey,metadataValue))) {
            metadataList = metadataList :+ (metadataKey, metadataValue)
          }
        }
        else{
          logger.warn("file: "+metadataFilePath+" should have 2 columns. Check this line that was excluded: "+line)
        }
      })

      val tempFile = metadataFilePath+".temp"
      val writer = new PrintWriter(tempFile)
      metadataList.sortWith((A,B) => A._1.compare(B._1)<0).foreach(metadata =>{
        writer.write(metadata._1 + "\t" + metadata._2 + "\n")
      })
      writer.close()

      try {
        import java.io.{File, FileInputStream, FileOutputStream}
        val src = new File(tempFile)
        val dest = new File(metadataFilePath)
        new FileOutputStream(dest) getChannel() transferFrom(
          new FileInputStream(src) getChannel, 0, Long.MaxValue)
        new File(tempFile).delete()
      }
      catch {
        case e: IOException => logger.error("could not change metadata key on the file " + metadataFilePath)
      }
    }
    replaced
  }
}
