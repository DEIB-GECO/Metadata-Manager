package it.polimi.genomics.metadata.step

import java.io._

import it.polimi.genomics.metadata.cleaner.RuleBase
import it.polimi.genomics.metadata.database.FileDatabase
import it.polimi.genomics.metadata.downloader_transformer.default.SchemaFinder
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler
import it.polimi.genomics.metadata.step.CleanerStep.{createSymbolicLink, logger}
import it.polimi.genomics.metadata.step.utils.DirectoryNamingUtil
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.{GetResult, PositionedResult}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try


object FlattenerStep extends Step {

  val GCM_CURATE_PREFIX = "gcm_curated__"

  object ResultMap extends GetResult[Map[String, Any]] {
    def apply(pr: PositionedResult) = {
      val rs = pr.rs // <- jdbc result set
      val md = rs.getMetaData();
      val res = (1 to pr.numColumns).map { i => md.getColumnName(i) -> rs.getObject(i) }.toMap
      res
    }
  }

  val excludeList = List("item_id",
    "experiment_type_id",
    "dataset_id",
    "case_id", "case_study_id", "project_id",
    "replicate_id", "biosample_id", "donor_id",
    ".*_tid")

  val columnNamesMap = Map("program_name" -> "source",
    "name" -> "dataset_name",
    "format" -> "file_format",
    "is_ann" -> "is_annotation",
    "type" -> "biosample_type",
    "bio_replicate_num" -> "biological_replicate_number",
    "tech_replicate_num" -> "technical_replicate_number",
    "external_ref" -> "external_reference")


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


              val fileList = inputFolder.listFiles.
                filter(_.isFile).
                filter(!_.getName.endsWith(".schema")).toList


              val datasetFileName = datasetOutputFolder + File.separator + "dataset_name.txt"
              val datasetFullName = scala.io.Source.fromFile(datasetFileName).mkString.trim


              fileList.foreach { file =>
                val fileName = file.getName


                val fullOutPath = flattenerFolder + File.separator + fileName

                if (fileName.endsWith(".meta")) {
                  val fileNameFirstPart = fileName.split("\\.").head
                  ruleBasePath.applyRBToFile(file.getAbsolutePath, fullOutPath)


                  val databaseLinesTuples = Try {
                    val query =
                      sql"""SELECT *
                          FROM dataset d
                          JOIN item i on d.dataset_id = i.dataset_id
                          LEFT JOIN experiment_type et on i.experiment_type_id = et.experiment_type_id
                          LEFT JOIN case2item c2i on i.item_id = c2i.item_id
                          LEFT JOIN case_study cs on c2i.case_id = cs.case_study_id
                          LEFT JOIN project p on cs.project_id = p.project_id
                          LEFT JOIN replicate2item r2i on i.item_id = r2i.item_id
                          LEFT JOIN replicate r on r2i.replicate_id = r.replicate_id
                          LEFT JOIN biosample b on r.biosample_id = b.biosample_id
                          LEFT JOIN donor d2 on b.donor_id = d2.donor_id
                          WHERE d.name = '#${datasetFullName}'
                          AND (
                              i.local_url ILIKE '%/#${fileNameFirstPart}/region'
                              OR (i.local_url IS NULL AND i.item_source_id ILIKE '#${fileNameFirstPart}')
                            )""".as(ResultMap)
                    val result = DbHandler.database.run(query)
                    val res = Await.result(result, Duration.Inf)
                    res
                      .flatten
                      .filterNot { case (col, _) => excludeList.exists(col.matches) }
                      .filterNot { case (_, value) => value == null }
                      .map { case (col, value) => (columnNamesMap.getOrElse(col, col), value) }
                  }.getOrElse(Seq.empty)
                    .to[mutable.Set]

                  def addCount(key: String, countKey: String, keepZero: Boolean = true): Unit = {
                    val count = databaseLinesTuples.count(_._1 == key)
                    if (keepZero || count > 0)
                      databaseLinesTuples += countKey -> count
                  }

                  addCount("biological_replicate_number","biological_replicate_count")
                  addCount("technical_replicate_number","technical_replicate_count")


                  val databaseLines = databaseLinesTuples
                    .map(t => GCM_CURATE_PREFIX + t.productIterator.mkString("\t"))



                  // always sort
                  // if(databaseLines.nonEmpty) {

                  //read file
                  val fileLines = scala.io.Source.fromFile(fullOutPath).getLines()
                    .filter(_.trim.nonEmpty)

                  val allLines = databaseLines ++ fileLines
                  val allLinesUniqSorted = allLines
                    //remove duplicates
                    .toSet
                    //to sort
                    .toList.sorted


                  val pw = new PrintWriter(new File(fullOutPath))
                  pw.write(allLinesUniqSorted.mkString(scala.compat.Platform.EOL))
                  pw.close()

                  // }
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
