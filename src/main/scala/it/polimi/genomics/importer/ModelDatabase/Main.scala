package it.polimi.genomics.importer.ModelDatabase


import scala.xml.XML
import scala.io.Source
import scala.collection.mutable.ListBuffer


object main extends App{
  val xml = XML.loadFile("/home/federico/IdeaProjects/file copia/target/scala-2.12/classes/setting.xml")


  //creo il mapping nel file di nacho, problema degli spazi
  val lines = Source.fromFile("/Users/federicogatti/IdeaProjects/Encode_Download/HG19_ENCODE/narrowPeak/Transformations/ENCFF001UYN.bed.meta").getLines.toArray
  var states = collection.mutable.Map[String, String]()
  var tables = new EncodeTables

  //itero sul contenuto di un enum
  val donor = new Donor
  val name : String = "DONOR"


  for(l <- lines){
    val first = l.split(" ", 2)
    states += (first(0) -> first(1))
  }


  var list = List(((xml \\ "table" \ "mapping" \ "source_key").text), ((xml \\ "table" \ "mapping" \ "global_key").text), (xml \\ "@name"))

  //creo la lista ordinata delle operazioni
  var operations = new ListBuffer[List[String]]()
  for (x <- xml \\ "table") {
    try {
      for(xi <- x \ "mapping") {
        var app = new ListBuffer[String]()
        app += ((x \ "@name").toString())
        app += ((xi \ "source_key").text)
        app += ((xi \ "global_key").text)
        operations += app.toList
      }
    } catch {
      case e: Exception => println("Source_key non trovata")
    }
  }
  val operationsList = operations.toList



  def makeOp(list: List[String], table: EncodeTable): Unit = list match{
    case Nil =>  println("Null")
   /* case "DONORS" :: source :: "source_id" :: Nil  => {
      printf("Donor " + source + " ")
      // val result =  check source
      //if(inserisco il sourceID)
        //
    }*/
    case "DONORS" :: source :: dest :: Nil  => {
      printf("Donor " + source + " " + dest)
     // makeOp(rest)
      table.setParameter(states(source),dest)
      //inserisco
    }
  /*  case "BIOSAMPLES" :: source :: dest :: Nil => {
      printf("Donor")
      //makeOp(rest)
    }
    case "REPLICATES" :: source :: dest :: Nil  => {
      printf("Donor")
     // makeOp(rest)
    }
    case "EXPERIMENTSTYPE":: source :: dest :: Nil   => {
      printf("Donor")
      //makeOp(rest)
    }
    case "PROJECTS" :: source :: dest :: Nil => {
      printf("Donor")
      //makeOp(rest)
    }
    case "CONTAINERS" :: source :: dest :: Nil  {
      printf("Donor")
     // makeOp(rest)
    }
    case "ITEMS" :: source :: dest :: Nil {
      printf("Donor")
      //makeOp(rest)
    }
    case "CASESITEMS" :: source :: dest :: Nil  {
      printf("Donor")
      //makeOp(rest)
    }*/

  }

  def populateTable(list: List[String], table: EncodeTable): Unit ={
    table.setParameter(states(list(2)),list(3))
  }

  //var current_table = "DONORS"
  var continue = true

  operationsList.map(x =>
    try {

     // val id = states(x(1))
      //controllo se l'id non è presente,
      //se non è presente eseguo l'inserimento di tutti e dati
      populateTable(x, tables.selectTableByName(x.head))
      //makeOp(x,tables.selectTableByName())

      //insertDonor
      //altrimenti passo al prossimo
    } catch {
      case e: Exception => println("Source_key non trovata")
  })


  /*if(checkInsertDonor(table.donor.sourceId)){
      donorId = DBHandler.insertDonor(donor.sourceId, donor.species, donor.age, donor.gender, donor.ethnicity)
  }
   */

  /*for (n <- nodes) {
    try {
      println(n.getClass)
    } catch {
      case e: Exception => println("Source_key non trovata")
    }
  }*/

  /*for (n <- nodes) {
    try {
      println(states(n.text.toString))
    } catch {
      case e: Exception => println("Source_key non trovata")
    }
  }*/




}
