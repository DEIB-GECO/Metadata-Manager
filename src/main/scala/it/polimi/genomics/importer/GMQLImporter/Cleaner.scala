package it.polimi.genomics.importer.GMQLImporter

import java.io._
import java.nio.file.{Files, Paths}

import it.polimi.genomics.importer.DefaultImporter.schemaFinder
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.utils.{DatasetNameUtil, DirectoryNamingUtil, ParameterUtil}
import it.polimi.genomics.importer.cleaner.RuleBase
import org.slf4j.{Logger, LoggerFactory}


object Cleaner extends Executable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def execute(source: GMQLSource, parallelExecution: Boolean): Unit = {
    if (source.transformEnabled) {

      logger.info("Starting cleaner for: " + source.outputFolder)
      val sourceId = FileDatabase.sourceId(source.name)

      //counters
      var modifiedRegionFilesSource = 0
      var modifiedMetadataFilesSource = 0
      var wrongSchemaFilesSource = 0
      //integration process for each dataset contained in the source.
      val integrateThreads = source.datasets.map((dataset: GMQLDataset) => {
        new Thread {
          override def run(): Unit = {
            val rulePathOpt = ParameterUtil.getParameter(dataset, "rule_base")
            if (dataset.transformEnabled) {
              val ruleBasePathOpt = rulePathOpt.map(new RuleBase(_))


              val t0Dataset: Long = System.nanoTime()
              var modifiedRegionFilesDataset = 0
              var modifiedMetadataFilesDataset = 0
              var wrongSchemaFilesDataset = 0
              var totalTransformedFiles = 0
              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)

              val datasetOutputFolder = dataset.fullDatasetOutputFolder
              //              val downloadsFolder = datasetOutputFolder + File.separator + "Downloads"
              val transformations2Folder = datasetOutputFolder + File.separator + DirectoryNamingUtil.transformFolderName
              val cleanerFolder = datasetOutputFolder + File.separator + DirectoryNamingUtil.cleanFolderName


              val folder = new File(cleanerFolder)
              if (folder.exists()) {
                Transformer.deleteFolder(folder)
              }


              logger.info("Starting download for: " + dataset.name)
              // puts the schema into the transformations folder.
              if (schemaFinder.downloadSchema(source.rootOutputFolder, dataset, cleanerFolder, source))
                logger.debug("Schema downloaded for: " + dataset.name)
              else
                logger.warn("Schema not found for: " + dataset.name)


              if (!folder.exists()) {
                folder.mkdirs()
                logger.debug("Folder created: " + folder)
              }
              logger.info("Cleaner for dataset: " + dataset.name)

              FileDatabase.delete(datasetId, STAGE.CLEAN)
              //id, filename, copy number.
              var filesToTransform = 0


              FileDatabase.getFilesToProcess(datasetId, STAGE.TRANSFORM).foreach { file =>
                val originalFileName: String =
                  if (file._3 == 1) file._2
                  else file._2.replaceFirst("\\.", "_" + file._3 + ".")

                val inputFilePath = transformations2Folder + File.separator + originalFileName
                val outFilePath = cleanerFolder + File.separator + originalFileName

                val fileId = FileDatabase.fileId(datasetId, inputFilePath, STAGE.CLEAN, originalFileName)

                if (inputFilePath.endsWith(".meta") && ruleBasePathOpt.isDefined) {
                  ruleBasePathOpt.get.applyRBToFile(inputFilePath, outFilePath)
                } else {
                  createSymbolicLink(inputFilePath, outFilePath)
                }

                if (true)
                  FileDatabase.markAsUpdated(fileId, new File(outFilePath).length.toString)
                else
                  FileDatabase.markAsFailed(fileId)

              }
              FileDatabase.markAsOutdated(datasetId, STAGE.CLEAN)

              FileDatabase.runDatasetTransformAppend(datasetId, dataset, filesToTransform, totalTransformedFiles)
              modifiedMetadataFilesSource = modifiedMetadataFilesSource + modifiedMetadataFilesDataset
              modifiedRegionFilesSource = modifiedRegionFilesSource + modifiedRegionFilesDataset
              wrongSchemaFilesSource = wrongSchemaFilesSource + wrongSchemaFilesDataset
              //              logger.info(modifiedRegionFilesDataset + " region data files modified in dataset: " + dataset.name)
              //              logger.info(modifiedMetadataFilesDataset + " metadata files modified in dataset: " + dataset.name)
              //              logger.info(wrongSchemaFilesDataset + " region data files do not respect the schema in dataset: " + dataset.name)
              //              val t1Dataset = System.nanoTime()
              //              logger.info(s"Total time for transformation dataset ${dataset.name}: ${Transformer.getTotalTimeFormatted(t0Dataset, t1Dataset)}")

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
          thread.start()
          thread.join()
        }
      }
      logger.info(modifiedRegionFilesSource + " region data files modified in source: " + source.name)
      logger.info(modifiedMetadataFilesSource + " metadata files modified in source: " + source.name)
      logger.info(wrongSchemaFilesSource + " region data files do not respect the schema in source: " + source.name)
      logger.info(s"Source ${source.name} transformation finished")
    }
  }

  def createSymbolicLink(fileInput: String, fileOutput: String) =
    Files.createSymbolicLink(Paths.get(fileOutput), Paths.get(fileInput))
}
