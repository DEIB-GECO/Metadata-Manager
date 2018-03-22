package it.polimi.genomics.importer.ModelDatabase.Utils

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import org.apache.log4j.Logger

object InsertMethodNew {
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val conf = ConfigFactory.load()
  private val characterSeparation = conf.getString("import.method_character_separation")

  def selectInsertionMethod(sourceKey: String, globalKey: String, methods: String, concatCharacter: String, subCharacter: String, newCharacter: String, remCharacter: String) = (actualParam: String, newParam: String) => {
    methods.split("-").foldLeft(newParam) {case (acc, method) => {
      method.toUpperCase() match {
        case "DEFAULT" => acc
        case "MANUALLY" => if (sourceKey == "null") null else sourceKey
        case "CONCAT" => if (actualParam == null) acc else actualParam.concat(concatCharacter + acc)
        case "CHECKPREC" => if (actualParam == null) acc else actualParam
        case "SELECTCASETCGA" => if (actualParam == null) DbHandler.getSourceSiteByCode(acc) else actualParam.concat(concatCharacter + DbHandler.getSourceSiteByCode(acc))
        case "REMOVE" => this.remove(remCharacter, acc)
        case "SUB" => this.replace(subCharacter, newCharacter, acc)
        case "UPPERCASE" => acc.toUpperCase()
        case "LOWERCASE" => acc.toLowerCase()
        case "ONTOLOGY" => sourceKey + '*' + acc
        case _ => {logger.error("Method " + method + " not found"); actualParam}
      }
    }}
  }

  private def substituteWordWith(phrase: String, wordToSub: String, wordToIns: String): String = {
    phrase.replace(wordToSub,wordToIns)
  }

  private def replaceAndConcat(actualParam: String, newParam: String, wordToSub: String, wordToIns: String, division: String): String  =  {
    if (actualParam == null) this.substituteWordWith(newParam, wordToSub, wordToIns) else actualParam.concat(division + this.substituteWordWith(newParam, wordToSub, wordToIns))
  }

  private def remove(remString: String, initialString: String): String = {
    remString.split(characterSeparation).foldLeft(initialString){(acc,i) => acc.replace(i,"")}
  }

  private def replace(subString: String, newString: String, initialString: String): String = {
    var newStringArray = newString.split(characterSeparation)
    subString.split(characterSeparation).foldLeft(initialString){(acc,i) => {
      val head = newStringArray.head
      newStringArray = newStringArray.drop(1)
      acc.replace(i,head)
    }}
  }
}
