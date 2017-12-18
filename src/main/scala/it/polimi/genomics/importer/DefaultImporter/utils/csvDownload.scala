package it.polimi.genomics.importer.DefaultImporter.utils

import java.io.File
import java.net.URL

import scala.sys.process._

class csvDownload(spreadsheetId: String, var spreadsheetName: String = ""){

  //private val url = s"https://docs.google.com/spreadsheets/d/$spreadsheetId/gviz/tq?tqx=out:"
  private val url:String = s" https://docs.google.com/spreadsheets/d/$spreadsheetId/export?format=csv&gid="
  if(spreadsheetName.isEmpty) spreadsheetName = spreadsheetId
  def get(sheetId:String, outPath:String = "", outName: String = "") = {
    val fileName = if(outName.isEmpty) spreadsheetName+"_"+sheetId else spreadsheetName+"_"+outName
    new URL(url+sheetId) #> new File(outPath + File.separator + s"$fileName.csv") !!
  }
}
