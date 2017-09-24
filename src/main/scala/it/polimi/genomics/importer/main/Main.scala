package it.polimi.genomics.importer.main


import akka.actor.Status.Success
import it.polimi.genomics.importer.RemoteDatabase.DbHandler

import scala.util.{Failure, Success}


object Main extends App {

  //DbHandler.insertDonor("ENCDO000AAL","homo sapiens", 18,"male","caucasian")

  val res = DbHandler.checkInsertDonor("ENCDO000AEL")
  println(res)


  println(DbHandler.getDonorId("ENCD333AAL"))
  //val db = DbHandler.setDatabase()


}
