package it.polimi.genomics.importer.ModelDatabase.Utils

import scala.xml.Node

class XMLSettingRetriver (default: String){

  def getMethod(xi: Node) : String = {
    if(xi.attribute("method").isDefined)
      (xi \ "@method").toString()
    else
      default
  }

  def getConcatCharacter(xi: Node) : String = {
    if(xi.attribute("concat_character").isDefined)
      (xi \ "@concat_character").toString()
    else
      " "
  }

  def getSubCharacter(xi: Node): String = {
    if(xi.attribute("sub_character").isDefined)
      (xi \ "@sub_character").toString()
    else
      ""
  }

  def getNewCharacter(xi: Node): String = {
    if(xi.attribute("new_character").isDefined)
      (xi \ "@new_character").toString()
    else
      ""
  }

  def getRemCharacter(xi: Node): String = {
    if(xi.attribute("rem_character").isDefined)
      (xi \ "@rem_character").toString()
    else
      ""
  }
}
