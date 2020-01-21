package it.polimi.genomics.metadata.util

import scala.xml.{Node, NodeSeq, XML}

/**
 * Created by Tom on ott, 2019
 *
 * Helper class to read the content of an XML file.
 *
 */
object XMLHelper {

  /**
   * This method has the purpose of returning a list of all the elements tagged with (or child of) a < tagName >
   * XML element.
   * ASSUMPTION: the element tagName contains only textual data without children nodes.
   * @param tagName name of an XML tag element contained in the XML file. The same element can occur even multiple times
   *                and at different nesting levels
   * @param pathToXMLFile the relative path to the XML file
   * @return a List of String, each one being the textual representation of whatever is in between < tagName >...< /tagName >.
   */
  def textTaggedWith(tagName: String, pathToXMLFile: String):List[String] ={
    val anXML = XML.loadFile(pathToXMLFile)
    (anXML \\ tagName).toList.map(node => node.text)
  }

  /**
   * This method has the purpose of returning a list of all the elements tagged with (or child of) a < tagName > XML
   * element, along with the type of the element, described by the given argument typeName.
   * ASSUMPTION: the element tagName contains only textual data without children nodes.
   * @param tagName name of an XML tag element contained in the XML file. The same element can occur even multiple times
   *                and at different nesting levels
   * @param typeName name of the attribute describing the type of the element with name tagName.
   * @param pathToXMLFile the relative path to the XML file
   * @return a List of pairs. Each pairs contains the textual representation of whatever is in between
   *         < tagName >...< /tagName > and the type of that element when the attribute typeName is defined, null otherwise.
   */
  def textAndTypeTaggedWith(tagName: String, typeName: String, pathToXMLFile: String):List[(String, String)] ={
    val anXML = XML.loadFile(pathToXMLFile)
    (anXML \\ tagName).toList.map(node => (node.text, node.attribute(typeName).getOrElse(List("null")).head.toString))
  }


}
