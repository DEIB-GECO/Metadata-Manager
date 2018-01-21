package it.polimi.genomics.importer.ModelDatabase.Utils

object InsertMethod {

  def selectInsertionMethod(sourceKey: String, globalKey: String, method: String) = (actualParam: String, newParam: String) => {
    method.toUpperCase() match {
      case "MANUALLY" => if(sourceKey == "null") null else sourceKey
      case "CONCAT-MANUALLY" => if(actualParam == null) sourceKey else actualParam.concat(" " + sourceKey)
      case "CONCAT" => if (actualParam == null) newParam else actualParam.concat(" " + newParam)
      case "CONCAT-NOSPACE" => if (actualParam == null) newParam else actualParam.concat(newParam)
      case "CHECK-PREC" => if(actualParam == null) newParam else actualParam
      case "DEFAULT" => newParam
      case _ => actualParam
    }
  }
}
