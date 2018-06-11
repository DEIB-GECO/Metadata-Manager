package it.polimi.genomics.importer.DefaultImporter.utils

import java.io._
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.CompressorStreamFactory

import scala.util.{Failure, Success, Try}


object Unzipper {

  private val ext = """\.[A-Za-z0-9]+$""".r
  private val archiveExt: List[String] = List(".tar", ".gz", ".a", ".ar", ".cpio", ".zip", ".xz", ".bz2", ".7z", ".arj",
    ".lzma", ".sz", ".z", ".arj")

  /**
    * extracts the gzipFile into the outputPath.
    *
    * @param gzipFile   full location of the gzip
    * @param outputPath full path of destination, filename included.
    */
  def unGzipIt(gzipFile: String, outputPath: String): Boolean = {
    val bufferSize = 1024
    val buffer = new Array[Byte](bufferSize)
    var unGzipped = false
    var timesTried = 0
    while (timesTried < 4 && !unGzipped) {
      try {
        val zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile)))
        val newFile = new File(outputPath)
        val fos = new FileOutputStream(newFile)

        var ze: Int = zis.read(buffer)
        while (ze >= 0) {

          fos.write(buffer, 0, ze)
          ze = zis.read(buffer)
        }
        fos.close()
        zis.close()
        unGzipped = true
      }
      catch {
        case _: IOException => timesTried += 1
      }
    }
    unGzipped
  }

  /**
    * detect the type of compressed file and return the appropriate stream to uncompress it.
    *
    * @param input  stream to read the compressed file
    * @return stream to read the uncompressed file
    */
  def uncompress(input: BufferedInputStream): InputStream =
    Try(new CompressorStreamFactory().createCompressorInputStream(input)) match {
      case Success(i) => new BufferedInputStream(i)
      case Failure(_) => input
    }

  /**
    * detect the type of archive and return the appropriate stream to extract it.
    *
    * @param input  stream to read the archive file
    * @return stream to read the extracted file
    */
  def extract(input: InputStream): ArchiveInputStream =
    new ArchiveStreamFactory().createArchiveInputStream(input)

  /**
    * extract and uncompress all the files contained in a compressed archive
    *
    * @param archivePath  path of a compressed archive
    * @param outputPath path of the directory in which place the extracted files
    * @param delateArchive if true the otiginal compressed archive is delated at the end of the process
    */
  def unpack(archivePath: String, outputPath: String, delateArchive: Boolean): Unit = {
    val input = extract(uncompress(new BufferedInputStream(new FileInputStream(archivePath))))
    //get all the archive entry as stream (lazy)
    def stream: Stream[ArchiveEntry] = input.getNextEntry match {
      case null  => Stream.empty
      case entry => entry #:: stream
    }

    //for each entry in the archive
    for(entry <- stream) {
      val archiveName = new File(archivePath).getName
      //this prevent accidental overwrite of the archive before the end of extraction
      val entryName = if (entry.getName == archiveName)
        "temp_" + entry.getName
      else
        entry.getName
      //if the archive contains a directory the corresponding directory in the output path must be created
      if (entry.isDirectory){
        import java.io.IOException
        import java.nio.file.{Files, Paths}
        val path = Paths.get(new File(outputPath, entryName).getAbsolutePath)
        //if directory exists?
        if (!Files.exists(path)) try
          Files.createDirectories(path)
        catch {
          case e: IOException => e
            //fail to create directory
        }
      }
      else {
        val output = new BufferedOutputStream(new FileOutputStream( new File(outputPath, entryName)))
        val buffer = new Array[Byte](1024)
        //read the file entry one byte at a time...
        var readed = input.read(buffer)
        var count = readed
        //... until the archive ends or all the entry byte are read
        while ( readed > 0 || count == entry.getSize) {
          output.write(buffer, 0, readed)
          readed = input.read(buffer)
          count += readed
        }
        output.close()
      }
    }
    input.close()
    if (delateArchive)
      new File(archivePath).delete()
  }

  /**
    * extract and uncompress all the files contained in a compressed archive recursively
    *
    * @param inputPath  path of a compressed archive
    * @param outputPath path of the directory in which place the extracted files
    * @param delateOriginal if true the otiginal compressed archive is delated at the end of the process
    */
  def unpackRecursive(inputPath: String, outputPath: String, delateOriginal: Boolean = false): Unit = {
    val input = new File(inputPath)
    //detect if the inputPath corresponds to a file or a directory
    if (input.isFile) { //if it is a file...
      val extension = ext.findFirstIn(input.getName).getOrElse("")
      if (archiveExt.contains(extension)) { //...and it is an archive
        unpack(inputPath, outputPath, delateOriginal) //extract its contents in outputPath
        unpackRecursive(outputPath, outputPath, delateOriginal = true)
      }
    }
    else { //if it is folder
      val files = getListOfFiles(inputPath)
      files.foreach(file => { //for each element contained in the folder
        if (file.isFile ) { //if it is a file
          val extension = ext.findFirstIn(file.getName).getOrElse("")
          if (archiveExt.contains(extension)) // if it is an archive
            unpackRecursive(file.getCanonicalPath, outputPath, delateOriginal = true)
        }
        else { //if it is a directory
          unpackRecursive(file.getAbsolutePath, outputPath + File.separator + file.getName, delateOriginal = true)
        }
      })
    }
  }

  /**
    * return the file conteined in a directory.
    *
    * @param dir  directory path
    * @return list of file contained in dir
    */
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.toList
    } else {
      List[File]()
    }
  }
}