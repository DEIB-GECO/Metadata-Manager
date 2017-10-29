package it.polimi.genomics.importer.ModelDatabase


import it.polimi.genomics.importer.ModelDatabase.Utils.{ReplicateList, XMLReader}
import java.io._

import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import it.polimi.genomics.importer.main.program.{getTotalTimeFormatted, logger}
import org.apache.log4j._
import it.polimi.genomics.importer.GMQLImporter.schemaValidator

import scala.io.Source


object main{
  val logger: Logger = Logger.getLogger(this.getClass)
  private val r = ".*.bed.meta".r


  def main(args: Array[String]): Unit = {
    val console = new ConsoleAppender() //create appender
    //configure the appender
    val PATTERN = "%d [%p|%c|%C{1}] %m%n"
    console.setLayout(new PatternLayout(PATTERN))
    console.setThreshold(Level.ERROR)
    console.activateOptions()
    //add appender to any Logger (here is root)
    Logger.getRootLogger.addAppender(console)
    val console2 = new ConsoleAppender()
    //configure the appender
    console2.setLayout(new PatternLayout(PATTERN))
    console2.setThreshold(Level.INFO)
    console2.activateOptions()
    Logger.getLogger("it.polimi.genomics.importer").addAppender(console2)

    val console3 = new ConsoleAppender()
    //configure the appender
    console3.setLayout(new PatternLayout(PATTERN))
    console3.setThreshold(Level.WARN)
    console3.activateOptions()
    Logger.getLogger("slick").addAppender(console3)
    //BasicConfigurator.configure()
    if (args.length == 0) {
      logger.warn(s"No arguments specified")
    }
    else if( args.length != 2){
      logger.warn(s"Incorrect number of arguments")
      logger.info("GMQLImporter help:\n"
        + "\t Run with configuration_xml_path and gmql_conf_folder as arguments\n"
      )
    }
    else {
      val pathXML = args.head
      val pathGMQL = args.drop(1).head
      val schemaUrl = "/home/federico/IdeaProjects/GMQL-Importer/Example/xml/setting.xsd"
      if (schemaValidator.validate(pathXML, schemaUrl)){
        logger.info("Xml file is valid for the schema")
        DbHandler.setDatabase()
        val t0: Long = System.nanoTime()
        recursiveListFiles(new File(pathGMQL)).filter(f => r.findFirstIn(f.getName).isDefined).map(path => analizeFile(path.toString))
        //recursiveListFiles(new File("/home/federico/Encode_Download/")).filter(f=> r.findFirstIn(f.getName).isDefined).map(println)
        //analizeFile("/home/federico/_Encode_Download/HG19_ENCODE/broadPeak/Transformations/ENCFF015LNH.bed.meta")
        val t1 = System.nanoTime()
        logger.info(s"Total time for the run ${getTotalTimeFormatted(t0, t1)}")
      }
      else
        logger.warn("Xml file is not valid according the specified schema, check: " + schemaUrl)


    }
  }

  def analizeFile(path: String) {

    logger.info(s"Start to read $path")
    val lines = Source.fromFile(path).getLines.toArray
    var states = collection.mutable.Map[String, String]()
    var tables = new EncodeTables
    val replicateList = new ReplicateList(lines)

    for (l <- lines) {
      val first = l.split("\t", 2)
      states += (first(0) -> first(1))
    }

    val xml = new XMLReader("/home/federico/IdeaProjects/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/ModelDatabase/Utils/setting.xml", replicateList)


    val operationsList = xml.operationsList
    operationsList.map(x =>
      try {
        populateTable(x, tables.selectTableByName(x.head))

      } catch {
        case e: Exception => logger.warn(s"SourceKey does't find for $x")


      })
    tables.insertTables()

    def populateTable(list: List[String], table: Table): Unit = {
      val insertMethod = selectInsertionMethod(list(1),list(2),list(3))
      if(list(3).equals("MANUALLY"))
        table.setParameter(list(1), list(2), insertMethod)
      else
        table.setParameter(states(list(1)), list(2), insertMethod)
    }
  }


  def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }
  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def selectInsertionMethod(sourceKey: String, globalKey: String, method: String) = (actualParam: String, newParam: String) => {
    method.toUpperCase() match {
      case "MANUALLY" => if(sourceKey == "null") null else sourceKey
      case "CONCAT" => if (actualParam == null) newParam else actualParam.concat(" " + newParam)
      case "DEFAULT" => newParam
      case _ => actualParam
    }
  }
}

