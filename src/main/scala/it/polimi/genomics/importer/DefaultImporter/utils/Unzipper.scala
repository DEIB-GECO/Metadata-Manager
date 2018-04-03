package it.polimi.genomics.importer.DefaultImporter.utils

import java.io._
import java.util.zip.GZIPInputStream


object Unzipper {

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
        case e: IOException => timesTried += 1
      }
    }
    unGzipped
  }
}
