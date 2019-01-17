package it.polimi.genomics.metadata.step

import java.io.File

import it.polimi.genomics.metadata.cleaner.RuleBase
import it.polimi.genomics.metadata.database.FileDatabase
import it.polimi.genomics.metadata.downloader_transformer.default.SchemaFinder
import it.polimi.genomics.metadata.step.CleanerStep.{createSymbolicLink, logger}
import it.polimi.genomics.metadata.step.utils.DirectoryNamingUtil
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}

object FlattenerStep extends Step {
  override def execute(source: Source, parallelExecution: Boolean): Unit = {
    if (source.flattenerEnabled) {

      logger.info("Starting flattener for: " + source.outputFolder)
      val sourceId = FileDatabase.sourceId(source.name)

      //counters
      var modifiedRegionFilesSource = 0
      var modifiedMetadataFilesSource = 0
      var wrongSchemaFilesSource = 0
      //integration process for each dataset contained in the source.
      val integrateThreads = source.datasets.map((dataset: Dataset) => {
        new Thread {
          override def run(): Unit = {
            val rulePath = source.flattenerRulePath
            //  if (dataset.transformEnabled) {
            // flattener works at application level
            if (true) {
              val ruleBasePath = new RuleBase(rulePath)


              val t0Dataset: Long = System.nanoTime()
              var modifiedRegionFilesDataset = 0
              var modifiedMetadataFilesDataset = 0
              var wrongSchemaFilesDataset = 0
              var totalTransformedFiles = 0

              val datasetOutputFolder = dataset.fullDatasetOutputFolder
              val cleanerFolder = datasetOutputFolder + File.separator + DirectoryNamingUtil.cleanFolderName
              val flattenerFolder = datasetOutputFolder + File.separator + DirectoryNamingUtil.flattenFolderName


              val inputFolder = new File(cleanerFolder)
              if (!inputFolder.exists()) {
                throw new Exception("No input folder: " + cleanerFolder)
              }


              val folder = new File(flattenerFolder)
              if (folder.exists()) {
                TransformerStep.deleteFolder(folder)
              }


              logger.info("Starting flattener for: " + dataset.name)
              if (SchemaFinder.downloadSchema(source.rootOutputFolder, dataset, flattenerFolder, source))
                logger.debug("Schema flattener for: " + dataset.name)
              else
                logger.warn("Schema not found for: " + dataset.name)


              if (!folder.exists()) {
                folder.mkdirs()
                logger.debug("Folder created: " + folder)
              }
              logger.info("Flattener for dataset: " + dataset.name)

              var filesToTransform = 0


              val regionFileList = inputFolder.listFiles.
                filter(_.isFile).
                filter(!_.getName.endsWith(".schema")).toList


              regionFileList.foreach { file =>
                val fileName = file.getName


                val fullOutPath = flattenerFolder + File.separator + fileName

                if (fileName.endsWith(".meta")) {
                  ruleBasePath.applyRBToFile(file.getAbsolutePath, fullOutPath)
                } else {
                  createSymbolicLink(file.getAbsolutePath, fullOutPath)
                }

              }

              modifiedMetadataFilesSource = modifiedMetadataFilesSource + modifiedMetadataFilesDataset
              modifiedRegionFilesSource = modifiedRegionFilesSource + modifiedRegionFilesDataset
              wrongSchemaFilesSource = wrongSchemaFilesSource + wrongSchemaFilesDataset


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


}
