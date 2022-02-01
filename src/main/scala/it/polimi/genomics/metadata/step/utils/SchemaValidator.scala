package it.polimi.genomics.metadata.step.utils

import javax.xml.transform.stream
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import org.apache.log4j.Logger
import org.xml.sax.SAXException
/**
  * Created by Nacho on 12/14/16.
  */
object SchemaValidator {
  val logger: Logger = Logger.getLogger(this.getClass)


  /**
    * Checks with the corresponding schema the input xml file.
    * @param xmlFile Configuration xml file.
    * @param schemaUrl Http URL of the schema, now using
    *   "https://raw.githubusercontent.com/DEIB-GECO/GMQL-Importer/master/Example/xml/configurationSchema.xsd"
    * @return If the xmlFile follows the schema.
    */
  def validate(xmlFile: String, schemaUrl: String): Boolean = {
    try {
      val schemaLang = "http://www.w3.org/2001/XMLSchema"
      val factory = SchemaFactory.newInstance(schemaLang)
      val schema = factory.newSchema(new StreamSource(schemaUrl))
      val validator = schema.newValidator()
      validator.validate(new StreamSource(xmlFile))
      true
    } catch {
      case ex: SAXException =>
        logger.warn("SAXException", ex)
        false
      case ex: Exception =>
        logger.warn("Exception", ex)
        false
    }
  }
}
