package it.polimi.genomics.metadata.database

import it.polimi.genomics.metadata.step.xml.Dataset

/**
  * Created by Nacho
  * this object handles the uniqueness of the database and keeps it instantiated.
  * also works as a facade for the dbContainer.
  *
  * remember always to end the run and close the database after finishing its use.
  */
object FileDatabase {
  private val db = new DbContainer

  /**
    * connects to the database
    *
    * @param path storage path for the database
    */
  def setDatabase(path: String): Unit = {
    db.setDatabase(path)
  }

  /**
    * closes the connection to the database
    */
  def closeDatabase(): Unit = {
    db.closeDatabase()
  }

  /**
    * Tries to create a source with the given name and returns its id, if already exists is not replaced.
    *
    * @param name name of the source, should be unique.
    * @return id of the source.
    */
  def sourceId(name: String): Int = {
    db.sourceId(name)
  }

  /**
    * Tries to create a dataset and returns its id, if already exists is not replaced.
    *
    * @param sourceId dataset owner's id
    * @param name     name of the dataset should be unique for each source.
    * @return id of the dataset.
    */
  def datasetId(sourceId: Int, name: String): Int = {
    db.datasetId(sourceId, name)
  }

  /**
    * Tries to create a file and returns its id, if already exists is not replaced.
    *
    * @param datasetId     file owner's id.
    * @param url           origin url for the file.
    * @param stage         stage of the process the file is used Download/Transform.
    * @param candidateName the name the file should have.
    * @return id of the file.
    */
  def fileId(datasetId: Int, url: String, stage: Stage.Value, candidateName: String, useUrl: Boolean = true): Int = {
    db.fileId(datasetId, url, stage, candidateName, useUrl)
  }

  /**
    * Creates a run, with its general settings and the actual datetime.
    *
    * @param downloadEnabled  indicates if downloading was enabled during the run.
    * @param transformEnabled indicates if transforming was enabled during the run.
    * @param loadEnabled      indicates if loading was enabled during the run.
    * @param outputFolder     indicates the outputFolder defined as working directory.
    * @return the run's id.
    */
  def runId(downloadEnabled: String, transformEnabled: String, loadEnabled: String, outputFolder: String): Int = {
    db.runId(downloadEnabled, transformEnabled, loadEnabled, outputFolder)
  }

  /**
    * gets the last runId
    *
    * @return max id of runs
    */
  def getMaxRunNumber: Int = {
    db.getMaxRunNumber
  }

  /**
    * by receiving a run number, gets the previous one
    *
    * @param runNumber run number to check the previous
    * @return previous run number or 0 if no run before
    */
  def getPreviousRunNumber(runNumber: Int): Int = {
    db.getPreviousRunNumber(runNumber)
  }

  /**
    * counts the number of executions already run in the database
    *
    * @return count of runs
    */
  def getNumberOfRuns: Int = {
    var numberOfRuns = 0
    var runAux = getMaxRunNumber
    while (runAux > 0) {
      runAux = getPreviousRunNumber(runAux)
      numberOfRuns += 1
    }
    numberOfRuns
  }

  //--------------------------SECONDARY INSERTIONS RUNSOURCE/RUNDATASET/RUNFILE/PARAMETERS------------------------------

  /**
    * generates the last representation of the source in the last run.
    *
    * @param runId            id for the run.
    * @param sourceId         id for the source.
    * @param url              url for the source.
    * @param outputFolder     working directory for the source.
    * @param downloadEnabled  indicates if the source is being downloaded.
    * @param downloader       indicates the downloader used for the source.
    * @param transformEnabled indicates if the source is being transformed.
    * @param transformer      indicates the transformer used by the source.
    * @param loadEnabled      indicates if the source is bein loaded.
    * @param loader           indicates the loader used by the source.
    * @return the runSource id.
    */
  def runSourceId(runId: Int, sourceId: Int, url: String, outputFolder: String, downloadEnabled: String,
                  downloader: String, transformEnabled: String, transformer: String, loadEnabled: String, loader: String
                 ): Int = {
    db.runSourceId(sourceId, url, outputFolder, downloadEnabled, downloader, transformEnabled, transformer,
      loadEnabled, loader)
  }

  /**
    * Inserts the parameters used by a source
    *
    * @param runSourceId source who is using the parameters
    * @param description explains what the parameter is used for
    * @param key         indicates the name of the parameter
    * @param value       indicates the value of the parameter
    * @return id of the parameter.
    */
  def runSourceParameterId(runSourceId: Int, description: String, key: String, value: String): Int = {
    db.runSourceParameterId(runSourceId, description, key, value)
  }

  /**
    * generates the last representation of the dataset in the last run.
    *
    * @param runId            id for the run.
    * @param datasetId        id for the dataset.
    * @param outputFolder     working directory for the dataset.
    * @param downloadEnabled  indicates if the dataset is being downloaded.
    * @param transformEnabled indicates if the dataset is being transformed.
    * @param loadEnabled      indicates if the source is being loaded.
    * @param schemaUrl        indicates the url of the schema.
    * @param schemaLocation   indicates whether the schema is local or remote.
    * @return the runDataset id.
    */
  def runDatasetId(runId: Int, datasetId: Int, outputFolder: String, downloadEnabled: String, transformEnabled: String,
                   loadEnabled: String, schemaUrl: String, schemaLocation: String
                  ): Int = {
    db.runDatasetId(datasetId, outputFolder, downloadEnabled, transformEnabled, loadEnabled, schemaUrl, schemaLocation, Option(runId))
  }

  /**
    * prints the log for dataset downloaded files for a specified run
    *
    * @param runDatasetId identifies of the rundataset.
    */
  def printRunDatasetDownloadLog(runDatasetId: Int, datasetId: Int, runId: Int): Unit = {
    if (runDatasetId > 0) {
      db.printRunDatasetLog(runDatasetId, Stage.DOWNLOAD)
      db.printNewReadyFailedFiles(datasetId, runId)
    }
  }

  /**
    * prints the log for dataset transformed files for a specified run
    *
    * @param runDatasetId identifies of the rundataset.
    */
  def printRunDatasetTransformLog(runDatasetId: Int): Unit = {
    if (runDatasetId > 0)
      db.printRunDatasetLog(runDatasetId, Stage.TRANSFORM)
  }

  /**
    * Inserts the parameters used by a source
    *
    * @param runDatasetId dataset who is using the parameters
    * @param description  explains what the parameter is used for
    * @param key          indicates the name of the parameter
    * @param value        indicates the value of the parameter
    * @return id of the parameter.
    */
  def runDatasetParameterId(runDatasetId: Int, description: String, key: String, value: String): Int = {
    db.runDatasetParameterId(runDatasetId, description, key, value)
  }

  /**
    * feeds the total downloaded files and possible downloads for a dataset
    *
    * @param datasetId       dataset identifier in the database
    * @param dataset         dataset where the files are being downloaded
    * @param totalFiles      total number of files to be downloaded
    * @param downloadedFiles total of successful downloads out of totalFiles
    * @return id for the runDatasetLog created
    */
  def runDatasetDownloadAppend(datasetId: Int, dataset: Dataset, totalFiles: Int, downloadedFiles: Int): Int = {
    val runId = db.getMaxRunNumber
    val runDatasetId = db.runDatasetId(
      datasetId,
      dataset.outputFolder,
      dataset.downloadEnabled.toString,
      dataset.transformEnabled.toString,
      dataset.loadEnabled.toString,
      dataset.schemaUrl,
      dataset.schemaLocation.toString,
      Option(runId)
    )
    db.runDatasetLogId(runDatasetId, Stage.DOWNLOAD, totalFiles, downloadedFiles)
  }

  /**
    * feeds the total downloaded files and possible transforms for a dataset
    *
    * @param datasetId        dataset identifier in the database
    * @param dataset          dataset where the files are being transformed
    * @param totalFiles       total number of files to be transformed
    * @param transformedFiles total of successful transformed out of totalFiles
    * @return id for the runDatasetLog created
    */
  def runDatasetTransformAppend(datasetId: Int, dataset: Dataset, totalFiles: Int, transformedFiles: Int): Int = {
    val runId = db.getMaxRunNumber
    val runDatasetId = db.runDatasetId(
      datasetId,
      dataset.outputFolder,
      dataset.downloadEnabled.toString,
      dataset.transformEnabled.toString,
      dataset.loadEnabled.toString,
      dataset.schemaUrl,
      dataset.schemaLocation.toString,
      Option(runId)
    )
    db.runDatasetLogId(runDatasetId, Stage.TRANSFORM, totalFiles, transformedFiles)
  }

  /**
    * Generates the versioning for the metadata of the files.
    *
    * @param fileId indicats the file whose verions are.
    * @return id of the runFile.
    */
  def runFileId(fileId: Int): Int = {
    db.runFileId(fileId)
  }

  //-------------------------------Run closing--------------------------------------------------------------------------
  /**
    * puts the time finished of the run
    *
    * @param runId id for the run.
    */
  def endRun(runId: Int): Unit = {
    db.endRun(runId)
  }

  //------------------------------FILE OPERATIONS SECTION FILENAME/CHECKIFUPDATE/PROCESS--------------------------------
  /**
    * By receiving a candidate name returns a unique name inside the dataset.
    *
    * @param fileId id for the file.
    * @return unique name among the dataset's files. -1 as the Int indicates the file should not exist.
    */
  def getFileNameAndCopyNumber(fileId: Int): (String, Int) = {
    db.getFileNameAndCopyNumber(fileId)
  }

  /**
    * returns hash, size and last update.
    *
    * @param fileId identifier of the file.
    * @return hash, size and last update.
    */
  def getFileDetails(fileId: Int): (String, String, String) = {
    db.getFileDetails(fileId)
  }

  /**
    * returns hash, size and last update.
    *
    * @param fileId identifier of the file.
    * @return hash, size and last update.
    */
  def getFileAllDetails(fileId: Int) = {
    db.getFileAllDetail(fileId)
  }


  /**
    * indicates which is the maximum copy number for the same filename inside the same dataset.
    *
    * @param datasetId datast where the file belongs
    * @param fileName  original file name
    * @param stage     indicates whether download/transform
    * @return max copy number
    */
  def getMaxCopyNumber(datasetId: Int, fileName: String, stage: Stage.Value): Int = {
    db.getMaxCopyNumber(datasetId, fileName, stage)
  }

  /**
    * checks if the given file has to be updated based on its hash, size and last update.
    *
    * @param fileId           id for the file.
    * @param hash             hash of the file.
    * @param originSize       original size in the source.
    * @param originLastUpdate original last updated in the source.
    * @return true = has to be updated.
    */
  def checkIfUpdateFile(fileId: Int, hash: String, originSize: String, originLastUpdate: String): Boolean = {
    db.checkIfUpdateFile(fileId, hash, originSize, originLastUpdate)
  }

  /**
    * returns all the non outdated files with its copy number
    *
    * @param datasetId dataset from where files are required.
    * @return non outdated files id, name, copy number
    */
  def getFilesToProcess(datasetId: Int, stage: Stage.Value): Seq[(Int, String, Int)] = {
    db.getFilesToProcess(datasetId, stage)
  }

  /**
    * returns all the failed files with its copy number
    *
    * @param datasetId dataset from where files are required.
    * @return failed files id, name, copy number,url, hash
    */
  def getFailedFiles(datasetId: Int, stage: Stage.Value): Seq[(Int, String, Int, String, String)] = {
    db.getFailedFiles(datasetId, stage)
  }

  /**
    * marks indicated file as to be UPDATED.
    *
    * @param fileId identifier for the file.
    */
  def markAsUpdated(fileId: Int, size: String): Unit = {
    db.markAsUpdated(fileId, size)
  }

  /**
    * marks indicated file as to be UPDATED.
    *
    * @param fileId identifier for the file.
    */
  def markAsUpdated(fileId: Int, size: String, hash: String): Unit = {
    db.markAsUpdated(fileId, size, hash)
  }

  /**
    * to be used when the file download or transformation fails, puts file status into FAILED
    *
    * @param fileId identifier for the file.
    */
  def markAsFailed(fileId: Int): Unit = {
    db.markAsFailed(fileId)
  }

  /**
    * mark all files that have not been compared into the log as outdated.
    * meant to be used at the end of all comparisons (all check if udpate)
    * changes COMPARE to OUTDATED.
    *
    * @param datasetId identifier for the dataset.
    * @param stage     indicates whether refers to download or transformed files.
    */
  def markAsOutdated(datasetId: Int, stage: Stage.Value): Unit = {
    db.markAsOutdated(datasetId, stage)
  }

  /**
    * mark all the files with status NOTHING into status COMPARE
    * meant to be used to check which files have been deleted from the source.
    *
    * @param datasetId identifier for the dataset.
    * @param stage     indicates whether refers to download or transformed files.
    */
  def markToCompare(datasetId: Int, stage: Stage.Value): Unit = {
    db.markToCompare(datasetId, stage)
  }

  /**
    * mark all the files with status NOTHING into status COMPARE
    * meant to be used to check which files have been deleted from the source.
    *
    * @param datasetId identifier for the dataset.
    * @param stage     indicates whether refers to download or transformed files.
    */
  def delete(datasetId: Int, stage: Stage.Value): Unit = {
    db.delete(datasetId, stage)
  }

  /**
    * Gives the current status of a file
    *
    * @param datasetId dataset where the file belongs to
    * @param url       origin url for the file
    * @param stage     whether is download or transform.
    */
  def fileStatus(datasetId: Int, url: String, stage: Stage.Value): Option[FILE_STATUS.Value] = {
    db.fileStatus(datasetId, url, stage)
  }

  /**
    * returns the url of a specified file.
    *
    * @param fileName  name of the file.
    * @param datasetID dataset from where files are required.
    * @param stage     whether is download or transform.
    * @return the url of the file.
    */
  def getFileUrl(fileName: String, datasetID: Int, stage: Stage.Value): String = {
    db.getFileUrl(fileName, datasetID, stage)
  }

  /**
    * Returns the last date in which a file of the specified dataset is returned.
    *
    * @param datasetID identifier of the dataset.
    * @return the date of the last download.
    */
  def getLastDownloadDate(datasetID: Int): String = {
    db.getLastDownloadDate(datasetID)
  }
}


