package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}

import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.XMLHelper
import it.polimi.genomics.metadata.util.vcf.VCFMutation
import org.slf4j.{Logger, LoggerFactory}

/**
 * Public interface of SchemaAdapter.
 *
 * Created by Tom on ott, 2019
 */
trait MutationPrinterTrait {
  /**
   *
   * @param mutation   an KGMutation.
   * @param forSample  optional: the sample name for which the mutation is requested. This value is used when fetching
   *                   attributes of category FORMAT. See VCFFormatKeys for an overview of the attributes available.
   * @param biosamples optional: the list of all the sample names offered in the VCF file of origin for the given mutation.
   *                   This value is used when fetching attributes of category FORMAT. See VCFFormatKeys for an overview
   *                   of the attributes available.
   * @return a String of tab-separated values whose order and content reflects the schema file used to generate this
   *         object. Missing values for the mutation provided are encoded as null for numeric attributes and as empty
   *         strings for attributes of type string.
   */
  def formatMutation(mutation: KGMutation, forSample: Option[String] = None, biosamples: Option[IndexedSeq[String]] = None):String
}

/**
 * Simple implementation of MutationPrinterTrait which formats a mutation as a tab-separated string with some
 * typical and pre-defined attributes.
 */
class SimplePrinter extends MutationPrinterTrait {

  override def formatMutation(mutation: KGMutation, forSample: Option[String], biosamples: Option[IndexedSeq[String]]): String = {
    KGTransformer.makeTSVString(
      mutation.chr,
      mutation.left,
      mutation.right,
      mutation.strand,
      mutation.id,
      mutation.ref,
      mutation.alt,
      mutation.length,
      mutation.mut_type,
      mutation.info.mkString(";")
    )
  }

}

/**
 * SchemaAdapter is a MutationPrinterTrait formatting a mutation as a tab-separated string with the attributes
 * specified in an XML schema file. SchemaAdapter is defined as a skeleton class because its concrete implementation
 * is defined at run-time when executing SchemaAdapter.fromSchema.
 */
abstract class SchemaAdapter extends MutationPrinterTrait {

  override def formatMutation(mutation: KGMutation, forSample: Option[String] = None, biosamples: Option[IndexedSeq[String]] = None):String ={
    val normalizedValues = (getRawValues(mutation, forSample, biosamples) zip alternativeNullValues)
      .map( val_alternative => {
        if(val_alternative._1 != VCFMutation.MISSING_VALUE_CODE) val_alternative._1 else val_alternative._2
      })
    KGTransformer.makeTSVString(normalizedValues)
  }

  /**
   *
   * @param mutation   an KGMutation.
   * @param forSample  optional: the sample name for which the mutation is requested. This value is used when fetching
   *                   attributes of category FORMAT. See VCFFormatKeys for an overview of the attributes available.
   * @param biosamples optional: the list of all the sample names offered in the VCF file of origin for the given mutation.
   *                   This value is used when fetching attributes of category FORMAT. See VCFFormatKeys for an overview
   *                   of the attributes available.
   * @return a String of tab-separated values whose order and content reflects the schema file used to generate this
   *         object. Missing values for the mutation provided are encoded VCFMutation.MISSING_VALUE_CODE.
   */
  //  THE FOLLOWING SIGNATURE MUST MATCH THE ONE WRITTEN FROM METHOD SchemaAdapter.fromSchema
  def getRawValues(mutation: KGMutation, forSample: Option[String] = None, biosamples: Option[IndexedSeq[String]] = None):List[String]

  /**
   * Metadata-Manager requires to use the following convention for expressing null/empty/not-available region attributes:
   * a "false" String for the attributes declared in the XML parameter boolean_region_values
   * a "null" String for attributes of type STRING.
   * an empty String for attributes of any numeric type.
   * This method maps the attributes in the schema to their corresponding empty values with the convention above.
   *
   * @return a List of empty Strings or "null" Strings or "false" Strings.
   */
  //  THE FOLLOWING SIGNATURE MUST MATCH THE ONE WRITTEN FROM METHOD SchemaAdapter.fromSchema
  def alternativeNullValues:List[String]

}

object SchemaAdapter {

  val MISSING_STRING_CODE = ""
  val MISSING_NUMBER_CODE = "NULL"
  val MISSING_BOOLEAN_CODE = "false"

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * This code generates the sequence of method calls to be performed on each mutation by matching the given
   * attribute list to a list of method names.
   * @param regionAttrsFromSchema the list of attribute names in the same order as they appear in the region schema file.
   * @return a representation of the code performing such calls and ordering the results in a List.
   */
  private def codeMappingSchema(regionAttrsFromSchema: List[(String, String)]):Code = {
    val instructions = regionAttrsFromSchema.map( attr => attr._1.toUpperCase match {
      case "CHR" | "CHROM" | "CHROMOSOME" => Code.call("mutation.chr")
      case "START" | "POS" | "LEFT" => Code.call("mutation.left")
      case "STOP" | "END" | "RIGHT" => Code.call("mutation.right")
      case "STRAND" | "STR" => Code.call("mutation.strand")
      case "ID" => Code.call("mutation.id")
      case "REF" => Code.call("mutation.ref")
      case "ALT" => Code.call("mutation.alt")
      case "MUT_TYPE" => Code.call("mutation.mut_type")
      case "LEN" | "LENGTH" => Code.call("mutation.length")
      case "QUAL" | "QUALITY" => Code.call("mutation.qual")
      case "FILTER" => Code.call("mutation.filter")
      case optionalInfoOrFormatKey =>
        val quotedParameter = "\"" + optionalInfoOrFormatKey + "\""
        Code.call(
          // look for an INFO value with the given key
          "mutation.info.getOrElse(" + quotedParameter + ", {\n" +
            // else look for a FORMAT value with the given key
            "if (forSample.isDefined && biosamples.isDefined) {\n" +
            "mutation.format(forSample.get, biosamples.get).getOrElse(" + quotedParameter + ",\n" +
            // else I'm sorry :P
            "VCFMutation.MISSING_VALUE_CODE)\n" +
            "}\n" +
            "else VCFMutation.MISSING_VALUE_CODE})\n"
        )
    })
    Code.generateListFromInstructions(instructions)
  }

  /**
   * This code generates the sequence of instructions to build the list of alternative values to be used in place of
   * null/empty mutation attributes.
   * @param regionAttributes the ordered list of types of the attributes in the same order as the attributes
   *                                appear in the schema file.
   * @param booleanRegionAttrs optional string listing of region attributes to be treated as boolean, i.e. they have
   *                           value false when not defined.
   * @return a representation of the code filling a List with the null value corresponding to each attribute type.
   */
  private def codeGeneratingListOfAlternativeNullValues(regionAttributes: List[(String, String)], booleanRegionAttrs: Option[String]):Code ={
    val setOfBooleanAttributes = booleanRegionAttrs.getOrElse({
      logger.info("XML config parameter boolean_region_values not defined")
      ""}).split(",").map(field => field.trim).toSet
    logger.info("region attributes "+setOfBooleanAttributes+" will be treated as boolean values (i.e. false if not defined)")
    val instructions = regionAttributes.map( attr => {
      val name = attr._1
      val _type = attr._2
      Code.call("\"" + // remember that values of the list needs to be in between quotes
        (
          if (setOfBooleanAttributes.contains(name)) MISSING_BOOLEAN_CODE
          else if (_type == "STRING") MISSING_STRING_CODE
          else MISSING_NUMBER_CODE
          )
        + "\"")
    })
    Code.generateListFromInstructions(instructions)
  }

  /**
   * Evaluates the XML schema at the given file path and matches each attribute to the corresponding method invocation.
   * Finally it generates an implementation for the abstract methods of class SchemaAdapter. The resulting SchemaAdapter
   * can map the schema attributes to the corresponding values for any mutation without needing to evaluate the
   * attributes any more.
   *
   * @param pathToXMLSchema the path to a region schema file
   * @return an concrete implementation of the abstract class SchemaAdapter.
   */
  def fromSchema(pathToXMLSchema: String, dataset: Dataset):MutationPrinterTrait ={
    logger.info("begin parsing region schema")
    if(Files.notExists(Paths.get(pathToXMLSchema))) {
      throw new FileNotFoundException("Schema not found at path "+pathToXMLSchema)
      // Do not change the schemaUrl path. Instead, move manually the schema to the required path.
    }
    // read schema as list of attributes (attr name, attr type)
    val regionAttrsFromSchema: List[(String, String)] = XMLHelper
      .textAndTypeTaggedWith("field", "type", pathToXMLSchema)

    // generate function body
    val classDefinition = Code.call(
      "import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes._; \n" +
      "import it.polimi.genomics.metadata.util.vcf._; \n" +
      "new SchemaAdapter {\n " +

        //  THE FOLLOWING SIGNATURE MUST MATCH THE ONE IN THE DECLARATION OF SchemaAdapter.scala
        "override def getRawValues(mutation: KGMutation, forSample: Option[String] = None, biosamples: Option[IndexedSeq[String]] = None):List[String] = {\n"+
        codeMappingSchema(regionAttrsFromSchema).get +
        "}\n"+  // close method

        //  THE FOLLOWING SIGNATURE MUST MATCH THE ONE IN THE DECLARATION OF SchemaAdapter.scala
        "override val alternativeNullValues:List[String] = " +
        codeGeneratingListOfAlternativeNullValues(regionAttrsFromSchema, dataset.getParameter("boolean_region_values")).get +"\n"+

        "}"   // close class
    )
    val compiledCode = Code.compile(classDefinition)
    logger.info("done parsing region schema")
    compiledCode().asInstanceOf[SchemaAdapter]
  }

}

/**
 * This class is nothing more than a wrapper around type String, with the added benefit of clarifying the purpose and
 * context of the contained String. Code instances are generated through the companion object.
 *
 * @param code a string representing program instruction(s)/declaration(s).
 */
private class Code private(code: String) {
  val get:String = code
}
private object Code {

  /**
   * Generates a Code object holding the instructions provided.
   * @param function the instruction(s)/declaration(s) to be compiled when invoking the method Code.compile(code: Code).
   * @return an instance of Code.
   */
  def call(function: String):Code ={
    new Code(function)
  }

  /**
   * @param instructions a List of Code instruction(s)/declaration(s)
   * @return a Code object declaring a List filled with the provided codes. For example, given the instructions
   *         "x+2" and "x*2", the resulting Code will declare a Scala List as:
   *         List("x+2", "x*2")
   */
  def generateListFromInstructions(instructions: List[Code]):Code = {
    new Code("List(" + instructions.map(_.get).mkString(",") + ")\n")
  }

  /**
   *
   * @param code the Code to be compiled
   * @return an object of type () => Any implementing the given Code. The compiled code can be any function or class,
   *         in which case the class must exists before compile at least as abstract in order to be a recognised type
   *         and be used later.
   */
  def compile(code: Code): () => Any ={
    import reflect.runtime.{currentMirror => mirror}
    import tools.reflect.ToolBox
    val toolbox = mirror.mkToolBox()
//    println(code.get)     // uncomment to see the code before it gets compiled
    val tree = toolbox.parse(code.get)
    toolbox.compile(tree)
  }
}

/*
/**
 * Set of methods used to try-out the characteristics of by SchemaAdapter & friends.
 */
object SchemaAdapterTest {

  /**
   * Prints 10 mutations formatted as specified in the provided schema file. Optionally it can format and print only
   * the specified mutation line but it must belong to the specified
   * VCF file.
   * @param aVCFFilePath path to a VCF file
   * @param oneOfItsMutationLine optional mutation line from the same VCF file given as argument
   * @param schemaPath path to the region schema file
   */
  def TESTfromSchema(aVCFFilePath: String, oneOfItsMutationLine: Option[String] = None, schemaPath: String):Unit ={
    HeaderMetaInformation.updatePropertiesFromMetaInformationLines(aVCFFilePath)
    // initialize schema
    val schemaAdapter = SchemaAdapter.fromSchema(schemaPath)
    // print mutation with schema
    if(oneOfItsMutationLine.isDefined)
      VCFMutation.splitOnMultipleAlternativeMutations(oneOfItsMutationLine.get).map(KGMutation.apply).foreach(mutation => {
        println(schemaAdapter.formatMutation(mutation))
      })
    else {
      val reader = FileUtil.open(aVCFFilePath).get
      advanceAndGetHeaderLine(reader)
      for (i <- 0 to 9) {
        val mutationLine = reader.readLine()
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine).map(KGMutation.apply).foreach(mutation => {
          println(schemaAdapter.formatMutation(mutation))
        })
      }
      reader.close()
    }
  }

  def TESTWellFormedAttributesInMetaInformationLines(VCFFilePath: String):Unit ={
    // get list of INFO and FORMAT in meta-info lines
    var attrSet = Set.empty[String]
    val fieldCardinalityPattern = PatternMatch.createPattern("##(FORMAT|INFO)=<ID=(\\w+),.*")
    val reader = FileUtil.open(VCFFilePath).get
    var aLine = reader.readLine()
    while(aLine.startsWith("##")){
      val formatParts = PatternMatch.matchParts(aLine, fieldCardinalityPattern)
      if(formatParts.nonEmpty)
        attrSet += formatParts(1)
      aLine = reader.readLine()
    }
    reader.close()

    // scan file looking for previously undeclared attr
    val reader1 = FileUtil.open(VCFFilePath).get
    advanceAndGetHeaderLine(reader1)
    FileUtil.scanFileAndClose(reader1, line => {
      val mutation = new VCFMutation(line)
      // !!!!! REQUIRES VCFMutation.formatKeysAsString to be public
      if(!mutation.info.keySet.subsetOf(attrSet) || ! mutation.formatKeysAsString.split(VCFMutation.FORMAT_SEPARATOR).toSet.subsetOf(attrSet))
        println("mut "+mutation.pos+" has undeclared INFO or FORMAT attrs")
    }, setupReadProgressCanary(VCFFilePath))
    println("File completely scanned")
  }

  def advanceAndGetHeaderLine(reader: BufferedReader): String = {
    var headerLine = reader.readLine()
    while (headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    headerLine
  }

  def advanceAndGetFirstMutation(reader: BufferedReader): String ={
    var firstVariantLine = reader.readLine()
    while(firstVariantLine.startsWith("#")) {
      firstVariantLine = reader.readLine()
    }
    firstVariantLine
  }


  private def setupReadProgressCanary(fullFilePath: String): Option[ApproximateReadProgress] ={
    println("COUNTING LINES OF FILE: "+fullFilePath)
    FileUtil.countLines(fullFilePath) match {
      case Failure(_) =>
        println("COUNT OF LINES IN FILE FAILED")
        None
      case Success(value) =>
        Some(new ApproximateReadProgress(value, 10, ApproximateReadProgress.simpleProgressNotification()))
    }
  }
}

*/