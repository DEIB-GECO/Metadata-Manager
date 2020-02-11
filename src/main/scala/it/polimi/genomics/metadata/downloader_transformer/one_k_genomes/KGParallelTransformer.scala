package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.lang.Thread.UncaughtExceptionHandler

import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.KGTransformer.removeExtension
import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.{AsyncFilesWriter, QueueObserver}

/**
 * Enables concurrent transformation of the origin files. Up to max_concurrent_transformations number of origin region
 * files can be transformed simultaneously. If the number of region origin files to be transformed exceeds
 * max_concurrent_transformations, the exceeding transformations are paused until an already running transformation
 * finishes.
 * Origin metadata files are generated concurrently by the main thread after the last
 * region file transformation has started.
 *
 * With observe_writing_queue_size_at_rate it is possible to observe the amount of the write requests comprehensively
 * issued by all the running transformations.
 *
 * Created by Tom on dic, 2019
 */
class KGParallelTransformer extends KGTransformer {

  protected var writer:AsyncFilesWriter = _
  protected var writerThread:Thread = _

  protected var queueObserverThread:QueueObserver = _

  protected var uncaughtExceptionHandler:UncaughtExceptionHandler = _

  override def onBeforeTransformation(dataset: Dataset): Unit = {
    super.onBeforeTransformation(dataset)
    val maxConcurrentTransformations = dataset.getParameter("max_concurrent_transformations").getOrElse({
      logger.debug("PARAMETER max_concurrent_transformations NOT FOUND IN CONFIGURATION XML. USED DEFAULT VALUE 3")
      "3"}).toInt
    writer = new AsyncFilesWriter(false, true, true, maxConcurrentTransformations)
    writerThread = writer.start(true)
    queueObserverThread = setupQueueObserver(writer, dataset)
    uncaughtExceptionHandler = new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.debug("Exception on Thread "+t.getName, e)   // just log the exception
        Thread.getDefaultUncaughtExceptionHandler.uncaughtException(t, e)
      }
    }
  }

  /**
   * Transforms a single origin file like the parent class does, but spawns a thread for every region origin file. This
   * method is repeatedly called by the framework in a single thread (no concurrency handling needed).
   */
  override def transformRegion(sourceFilePath: String, transformationsDirPath: String): Boolean = {
    // append region data
    extractArchive(sourceFilePath, removeExtension(sourceFilePath)) match {
      case None => false
      case Some(_VCFPath) =>
        // the source file is completely transformed generating the target region files for all the samples available in the source file
        if(transformedVCFs.add(_VCFPath)){
          val runnable = getVCFAdapter(_VCFPath).appendAllMutationsBySampleRunnable(transformationsDirPath, writer)
          writer.addJob(runnable)    // this instruction can possibly block the caller
          val thread = new Thread(runnable)
          thread.setName(runnable.toString)
          thread.setUncaughtExceptionHandler(uncaughtExceptionHandler)
          thread.start()
        }
        true
    }
  }

  override def onAllTransformationsDone(dataset: Dataset, priorPostprocessing: Boolean): Unit = {
    /* Depending on the amount of files to transform, it can take minutes before this callback is invoked even if all the
    * origin VCFs have been transformed. Indeed, the user of this class has a cycle calling transformRegion and
    * transformMetadata for all the samples. */
    super.onAllTransformationsDone(dataset, priorPostprocessing)
    logger.info("WAITING FOR ALL REGION TRANSFORMS TO FINISH")
    writerThread.join()
    dismissQueueObserver(queueObserverThread)
    logger.info("BEGIN OF POST-PROCESSING OPS")
  }

  private def setupQueueObserver(forAsyncFilesWriter: AsyncFilesWriter, dataset: Dataset): QueueObserver ={
    val queueObserverRate = dataset.getParameter("observe_writing_queue_size_at_rate").getOrElse("0").toInt
    var queueObserver:QueueObserver = null
    if(queueObserverRate > 0) {
      queueObserver = forAsyncFilesWriter.getQueueObserver(queueObserverRate)
      queueObserver.setDaemon(true)
      queueObserver.start()
    }
    queueObserver
  }

  private def dismissQueueObserver(queueObserver:QueueObserver): Unit ={
    if(queueObserver != null) {
      queueObserver.turnOff()
    }
  }
}
