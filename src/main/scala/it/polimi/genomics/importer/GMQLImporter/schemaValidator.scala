package it.polimi.genomics.importer.GMQLImporter
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import javax.xml.validation.SchemaFactory
import javax.xml.validation.{Validator=>JValidator}
import org.xml.sax.SAXException
/**
  * Created by nachon on 12/14/16.
  */
object schemaValidator {
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
      case ex: SAXException =>  false
      case ex: Exception =>  false
    }
  }
}
