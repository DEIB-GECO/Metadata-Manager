package it.polimi.genomics.metadata.util

import java.io.{BufferedWriter, IOException}
import java.nio.file.InvalidPathException
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, ConcurrentHashMap, TimeUnit}

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * This class provides methods to allow multiple threads to write onto the same files concurrently.
 * The standard usage is:
 * add target files
 * start (AsyncFilesWriter starts a thread listening for write requests)
 * (users call write on this class whenever they need)
 * stop (AsyncFilesWriter will write the requests already issued but it won't accept new requests. Finally it closes all the streams.)
 *
 * Optionally, it's possible to register the users with addJob methods and let AsyncFilesWriter to automatically end
 * its execution when all the users have terminated the writing operations and called removeJob. AsyncFilesWriter
 * accepts a maxJobs number of jobs concurrently running; further attempts to add a job before another job is removed,
 * will temporary block the requesting user's thread until another job is removed. A blocked user, is resumed automatically
 * when the issued job can be accepted.
 * NOTE that AsyncFilesWriter doesn't prevent duplicate jobs to be registered.
 *
 * If AsyncFilesWriter can't consume write requests at a sufficiently high rate, some users may be temporary blocked i.e.
 * any thread calling write may halt without receiving any exception and resume automatically after AsyncFilesWriter
 * completed some requests. You can also observe the status of the queue at runtime by invoking the method
 * getQueueObserver and running the obtained Runnable in a separate Thread.
 *
 * Writing occurs in FIFO order.
 *
 * Created by Tom on nov, 2019
 */
class AsyncFilesWriter(appendIfExists:Boolean, startOnNewLine: Boolean, autoNewLine:Boolean, maxJobs:Int) extends Runnable {

  val MESSAGE_QUEUE_CAPACITY = 2048

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // write buffers and target files
  private val targetFiles: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]
  private var writeBuffers: mutable.AnyRefMap[String, BufferedWriter] = mutable.AnyRefMap.empty[String, BufferedWriter]

  // messages
  private val messageQueue: BlockingQueue[Message] = new ArrayBlockingQueue[Message](MESSAGE_QUEUE_CAPACITY)
  private val messagePool:GenericObjectPool[Message] = {
    val poolConfig = new GenericObjectPoolConfig[Message]
    poolConfig.setMaxTotal(-1)  // unlimited (actually the number of requested objects is already regulated by the queue which can lock producer threads
    poolConfig.setMaxIdle(100)
    poolConfig.setTestOnReturn(false)
    poolConfig.setTestOnBorrow(false)
    poolConfig.setTestOnCreate(false)
    poolConfig.setTestWhileIdle(false)
    new GenericObjectPool[Message](new BasePooledObjectFactory[Message] {
      override def create(): Message = {new Message("", "")}

      override def wrap(t: Message): PooledObject[Message] = {
        new DefaultPooledObject[Message](t)
      }
    }, poolConfig)
  }

  // threads
  @volatile private var active:Boolean = false
  @volatile private var acceptWriteRequests:Boolean = false
  private var runningThread:Thread = _
  private var stopWhenJobsTerminate: Boolean = false
  private val runningJobs:BlockingQueue[Runnable] = new ArrayBlockingQueue[Runnable](maxJobs)


  def addTargetFile(uniqueName:String, targetFilePath:String): Unit ={
    targetFiles.putIfAbsent(uniqueName, targetFilePath)
  }

  def addTargetFiles(uniqueNames: IndexedSeq[String], targetFilePaths: IndexedSeq[String]):Unit ={
    (uniqueNames zip targetFilePaths).foreach { tuple => targetFiles.putIfAbsent(tuple._1, tuple._2) }
  }

  def addJob(job: Runnable): Unit ={
    try {
        runningJobs.put(job)
        logger.info("ASYNC WRITER: JOB REGISTERED: " + job.toString + ". JOBS: " + runningJobs.size())
    } catch {
      case e: InterruptedException => logger.debug("ASYNC WRITER INTERRUPTED EXCEPTION", e)
    }
  }

  def addJobs(jobs: Traversable[Runnable]):Unit ={
    jobs.foreach(job => {
      addJob(job)
    })
  }

  def removeAllJobs(): Unit ={
    try {
      runningJobs.clear()
      if(stopWhenJobsTerminate)
        stop()
      logger.info("ASYNC WRITER: ALL JOBS REMOVED")
    } catch {
      case e: InterruptedException => logger.debug("ASYNC WRITER: ERROR WHILE REMOVING ALL JOBS", e)
    }
  }

  def removeJob(job: Runnable): Unit = {
    try {
      if(runningJobs.remove(job))
        logger.info("ASYNC WRITER: JOB DONE: "+job.toString+". REMAINING JOBS: "+runningJobs.size())
      else
        logger.error("ASYNC WRITER: ATTEMPT TO REMOVE AN UNSEEN BEFORE JOB. JOB'S LIST UNCHANGED. THIS MAY PREVENT " +
          "ASYNC WRITER FROM TERMINATING.")
      if(stopWhenJobsTerminate && runningJobs.isEmpty){
        logger.info("ASYNC WRITER: ALL JOBS ARE FINISHED")
        stop()
      }
    } catch {
      case e: InterruptedException => logger.debug("ASYNC WRITER: ERROR WHILE REMOVING JOB "+job.toString, e)
    }
  }

  def write(content:String, uniqueNameAddress:String): Unit = {
    if(acceptWriteRequests) {
      try {
        val message = messagePool.borrowObject()
        messageQueue.put(message.set(content, uniqueNameAddress))
      } catch {
        case e: InterruptedException => logger.debug("ASYNC WRITER INTERRUPTED EXCEPTION", e)
      }
    } else {
      logger.debug("ASYNC WRITER: WRITE REQUEST REJECTED BECAUSE ASYNC WRITER IS NOT ACTIVE.")
    }
  }

  def start(stopWhenJobsTerminate: Boolean = false):Thread ={
    if (!active) {
      active = true
      acceptWriteRequests = true
      this.stopWhenJobsTerminate = stopWhenJobsTerminate
      runningThread = new Thread(this)
      runningThread.start()
      logger.info("ASYNC WRITER: STARTED ON THREAD " + runningThread.getName)
      runningThread
    } else {
      logger.info("ASYNC WRITER ALREADY STARTED")
      runningThread
    }
  }

  def stop():Unit ={
    acceptWriteRequests = false
    if(active) {
      logger.info("ASYNC WRITER: WAITING WRITES IN QUEUE TO FINISH")
      while(messageQueue.size()!=0)
        Thread.sleep(1000)
      active = false
      logger.info("QUEUE STATUS: " + messageQueue.size() + " LEFT ELEMENTS")
      runningThread = null
      writeBuffers.values.foreach(writer => try {
        writer.close()
      } catch {
        case e: java.io.IOException => logger.debug("STREAM CLOSING EXCEPTION", e)
      })
    } else {
      logger.info("ASYNC WRITER ALREADY STOPPED")
    }
  }

  override def run(): Unit = {
    logger.info("ASYNC WRITER: RUNNING ON THREAD "+Thread.currentThread().getName)
    while (active) {
      try {
        val message = messageQueue.poll(500, TimeUnit.MILLISECONDS)
        if (message != null) {
          if (writeBuffers.get(message.address).isDefined) {
            val writer = writeBuffers(message.address)
            if (autoNewLine)
              writer.newLine()
            writer.write(message.body)
          } else { // it's the first write for this target name
            val targetFilePath = targetFiles.get(message.address) // get target path
            if (targetFilePath != null) {
              val writer = if (appendIfExists) FileUtil.writeAppend(targetFilePath, startOnNewLine).get else FileUtil.writeReplace(targetFilePath).get
              writeBuffers += (message.address -> writer)
              writer.write(message.body)
            } else {
              logger.debug("TARGET NAME " + message.address + " HASN'T A CORRESPONDING FILE PATH. MAKE SURE TO CALL " +
                "addTargteFile BEFORE write! WRITE ON " + message.address + " ABORTED")
            }
          }
          //          logger.info("ELEMENTS IN POOL: "+messagePool.getNumActive + " ACTIVE "+messagePool.getNumIdle+" IDLE "+messagePool.getNumWaiters+" WAITERS")
          messagePool.returnObject(message)
        }
      } catch {
        case interrupted: InterruptedException => logger.debug("ASYNC WRITER INTERRUPTED EXCEPTION", interrupted)
        case path: InvalidPathException => logger.debug("ASYNC WRITER PATH EXCEPTION", path)
        case security: SecurityException => logger.debug("ASYNC WRITER SECURITY EXCEPTION", security)
        case io: IOException => logger.debug("ASYNC WRITER IO EXCEPTION", io)
      }
    }
    logger.info("ASYNC WRITER: STOPPED")
  }

  def getQueueObserver(setUpdateIntervalSeconds:Int): QueueObserver = {
    new QueueObserver(this, logger, setUpdateIntervalSeconds)
  }

  def isRunning: Boolean ={
    active
  }

  def instantQueueLength: Int ={
    messageQueue.size()
  }

//  class QueueObserver(setUpdateIntervalSeconds:Int) extends Thread {
//    private var _active = true
//
//    override def run(): Unit = {
//      while(_active){
//        try {
//          Thread.sleep(setUpdateIntervalSeconds*1000)
//          logger.info("QueueObserver: queue size "+messageQueue.size())
//        } catch {
//          case _: InterruptedException =>
//        }
//      }
//      logger.debug("Queue observer: interrupted")
//    }
//
//    override def interrupt(): Unit = {
//      _active = false
//      super.interrupt()
//    }
//  }

}

class QueueObserver protected[util] (private val asyncFilesWriter: AsyncFilesWriter, logger: Logger, setUpdateIntervalSeconds:Int) extends Thread {

  @volatile private var active:Boolean = false

  override def run(): Unit = {
    active = true
    while (active) {
      try {
        Thread.sleep(setUpdateIntervalSeconds * 1000)
      } catch {
        case _: InterruptedException =>
          /* calling interrupt on a sleeping/waiting thread throw an exception without side effects. Here we
          * catch the exception and complete the normal behaviour of interrupt by calling it a second time. This time
          * interrupt will set the interrupted flag.  */
          Thread.currentThread().interrupt()
          turnOff()
      }
      if(asyncFilesWriter.isRunning)
        logger.info("QueueObserver: queue length " + asyncFilesWriter.instantQueueLength)
    }
    logger.info("Queue observer: interrupted")
  }

  def turnOff(): Unit ={
    active = false
  }

}


class Message(var body:String,var address:String) {
  def set(body:String, address:String): Message ={
    this.body = body
    this.address = address
    this
  }
}



