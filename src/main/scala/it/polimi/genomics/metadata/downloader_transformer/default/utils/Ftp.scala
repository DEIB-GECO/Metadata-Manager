package it.polimi.genomics.metadata.downloader_transformer.default.utils

//https://gist.github.com/owainlewis/06e8bdfa6c42acec2ef9dec756c05c2f
//I use as interface for FTP client.
//package io.forward.ftp

import java.io.{File, FileOutputStream, IOException, InputStream}

import org.apache.commons.net.ftp._

import scala.util.Try

/**
  * https://gist.github.com/owainlewis/06e8bdfa6c42acec2ef9dec756c05c2f
  */
final class Ftp() {

  val client: FTPClient = new FTPClient
  def Client: FTPClient = client
  def login(username: String, password: String): Try[Boolean] = Try {
    client.login(username, password)
  }
  def reply: Int = client.getReplyCode
  def replyString: String = client.getReplyString

  def connect(host: String): Try[Unit] = Try {
    client.connect(host)
    client.enterLocalPassiveMode()
    //    client.setControlKeepAliveTimeout(300)
    client.setBufferSize(1024*1024)
    client.setFileType(FTP.BINARY_FILE_TYPE)
  }

  def connected: Boolean = client.isConnected

  def disconnect(): Unit = client.disconnect()

  def canConnect(host: String): Boolean = {
    client.connect(host)
    val connectionWasEstablished = connected
    client.disconnect()
    connectionWasEstablished
  }
  def listDirectories():Array[FTPFile]={
    client.listDirectories()
  }
  def listFiles(dir: Option[String] = None): List[FTPFile] =
    dir.fold(client.listFiles)(client.listFiles).toList

  def connectWithAuth(host: String,
                      username: String = "anonymous",
                      password: String = "") : Try[Boolean] = {
    var result = for {
      connection <- connect(host)
      login      <- login(username, password)
    } yield login
    client.setControlKeepAliveTimeout(300)
    client.setBufferSize(1024*1024)
    try {
      client.setFileType(FTP.BINARY_FILE_TYPE)
    }
    catch{
      case ex: IOException => result = Try{false}
    }
    result
  }

  def cd(path: String): Try[Boolean] = Try {
    client.changeWorkingDirectory(path)
  }

  def filesInCurrentDirectory: Seq[String] =
    listFiles().map(_.getName)

  def downloadFileStream(remote: String): InputStream = {
    val stream = client.retrieveFileStream(remote)
    client.completePendingCommand()
    stream
  }
  def workingDirectory(): String ={
    client.printWorkingDirectory()
  }

  def downloadFile(remote: String, local: String): Try[Boolean] = Try{
    val os = new FileOutputStream(new File(local))
    client.setDataTimeout(355)
    val res =
      try {
        client.retrieveFile(remote, os)
      }
      catch {
        case _: IOException => false
        case _: org.apache.commons.net.ftp.FTPConnectionClosedException => false
        case _: org.apache.commons.net.io.CopyStreamException => false
      }
    os.close()
    res
  }

  def uploadFile(remote: String, input: InputStream): Boolean =
    client.storeFile(remote, input)
}