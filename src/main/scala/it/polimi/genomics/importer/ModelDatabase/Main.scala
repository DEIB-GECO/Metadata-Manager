package it.polimi.genomics.importer.ModelDatabase


import it.polimi.genomics.importer.ModelDatabase.Utils.XMLReader
import org.apache.log4j.BasicConfigurator
import it.polimi.genomics.importer.RemoteDatabase.DbHandler

import scala.io.Source


object main extends App{

  //DbHandler.setDatabase()


 // BasicConfigurator.configure()
  val xml = new XMLReader("/Users/federicogatti/IdeaProjects/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/ModelDatabase/Utils/setting.xml")

  //creo il mapping nel file di nacho, problema degli spazi
  val lines = Source.fromFile("/Users/federicogatti/IdeaProjects/Encode_Download/HG19_ENCODE/broadPeak/Transformations/ENCFF718ITI.bed.meta").getLines.toArray
  var states = collection.mutable.Map[String, String]()
  var tables = new EncodeTables


  for(l <- lines){
    val first = l.split("\t", 2)
   // println(first(0) + " " + first(1))
    states += (first(0) -> first(1))
  }


  val operationsList = xml.operationsList



 /* def setForeignKeys(table: Table): Unit = {
    table.setForeignKeys()
  }

  def insertTableIntoDatabase(table: Table): Unit ={
    if(table.checkInsert())
      table.insert()
  }*/
  //var current_table = "DONORS"


  operationsList.map(x =>
    try {
      populateTable(x, tables.selectTableByName(x.head))
     // println(x.head)

    } catch {
      case e: Exception => println("Source_key non trovata: " + x)
  })
  tables.insertTables()
  def populateTable(list: List[String], table: Table): Unit ={
    table.setParameter(states(list(1)),list(2))
  }


  /*println(states("replicates__1__library__biosample__donor__accession"))
  println(states(operationsList(0)(1)))
  tables.selectTableByName(operationsList(0)(0)).setParameter(states(operationsList(0)(1)),operationsList(0)(2))
  val donor : Donor =  tables.selectTableByName(operationsList(0)(0)).asInstanceOf[Donor]
  println(donor.sourceId)*/
  //tables.selectTableByName("DONORS").setParameter("")
}
