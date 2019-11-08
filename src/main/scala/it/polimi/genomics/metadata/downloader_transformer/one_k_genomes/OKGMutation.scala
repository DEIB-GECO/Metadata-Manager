package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import it.polimi.genomics.metadata.util.vcf.{VCFInfoKeys, VCFMutation, VCFMutationTrait}
import org.slf4j.{Logger, LoggerFactory}

/**
 * This class extends the general VCFMutationTrait with methods and attributes whose implementation is specific for 1000Genomes,
 * since they are not mandatory VCF attributes and so their definition depends on the custom attributes specified in the file
 * by each source.
 *
 * Example cases are mutation's end and length.
 */
class OKGMutation(m: VCFMutationTrait) extends VCFMutationTrait {

  private var _ref:String = _
  private var _alt:String =  _
  private var _length: Long = -1  // DIFFERENCE IN LENGTH BETWEEN REF AND ALT ALLELES
  private var _left: Long = -1
  private var _right: Long = -1
  private var _type: Option[String] = None

  /**
   * This method simplifies REF and ALT alleles present in the original mutation and performs a conversion of coordinates
   * from 1-based to 0-based. It also computes right and left breakpoints, length and type of mutation.
   */
  {
    import VCFInfoKeys._
    // SVs
    if(m.info.get(SV_TYPE).isDefined) {
      _left = m.pos.toLong // "the POS field should specify the 1-based coordinate of the base before the SV or the best estimate thereof"
      // SVs
      if (m.info.get(SV_END).isDefined)
        _right = m.info(SV_END).toLong
      if (m.info.get(SV_LENGTH).isDefined) {
        // SV_LENGTH is negative for SVs representing long deletions
        _length = m.info(SV_LENGTH).toLong
      } else if( _right != -1)
        _length = _right - _left + 1
      reduceSV()
    } else {
      _left = m.pos.toLong - 1
      // INDELs
      if (/*m.info.get(VARIANT_TYPE).isDefined && m.info(VARIANT_TYPE).contains("I") ||*/
        m.info.get(SV_TYPE).isEmpty && m.ref.length != m.alt.length ) {
        _type = Some(OKGMutation.TYPE_INDEL)
        // In INDELS, ALT and REF always share the first or the last base.
        _length = Math.max(m.ref.length, m.alt.length) -1
        _right = if(m.alt.length < m.ref.length)   // deletion
          _left + m.ref.length
         else  // insertion
          _left
        reduceINDEL()
      } // SNPs
      else if (/*m.info.get(VARIANT_TYPE).isDefined && m.info(VARIANT_TYPE).startsWith("S") ||*/
        m.info.get(SV_TYPE).isEmpty && m.ref.length == 1 && m.alt.length == 1) {
        _type = Some(OKGMutation.TYPE_SNP)
        _length = 1
        _right = _left + 1
        copyREF_ALT()
      } // MNPs
      else if (m.info.get(SV_TYPE).isEmpty && m.ref.length == m.alt.length ) {
        _type = Some(OKGMutation.TYPE_MNP)
        // MNP mutations 're like concatenated SNPs
        // skip _length and _right initialization because they're then overwritten in reduceMNP()
        reduceMNP()
      }
    }
    if(_left == -1 || _length == -1 || _right == -1) {
      OKGMutation.logger.info(s"UNHANDLED VARIANT: $mutationStringWithoutFormat \n left: ${_left} length: ${_length} right: ${_right}")
      _right = _left
      if(_type.isEmpty)
        reduceSV()
      else
        copyREF_ALT()
    }
  }

  /**
   * In SVs, the base in REF is the base preceding the event described in ALT. It can be removed. Breakpoints or length
   * don't require any adjustment.
   */
  private def reduceSV():Unit ={
    if(m.ref.length==1 && !m.alt.startsWith("<"))
      OKGMutation.logger.debug("UNHANDLED SV: "+mutationStringWithoutFormat)
    _ref = if(m.ref.length == 1) VCFMutation.MISSING_VALUE_CODE else m.ref
    _alt = m.alt
  }

  /**
   * Indels always share the first base before the mutation, except for when the mutation occurs at position 1, in
   * which case the last base is shared instead of the first.
   * This method removes the common base and consequently adjusts the breakpoints and length.
   * If any between REF and ALT becomes empty, it's replaced with a dot.
   *
   * It may happen in microsatellite mutations to have INDELs whose shared bases are 2,3,... but that doesn't allows
   * us to delete all the common alleles because VCF doesn't provide base alignment. This concept can be clarified with
   * the following example: being ref:ACC	and alt:AC,A one can think that ACC -> AC corresponds to the deletions of the
   * last C, however it could also mean the deletion of the C in the middle, thus reducing ACC -> AC to C -> . would be
   * simply wrong.
   */
  private def reduceINDEL():Unit ={
    _ref = m.ref
    _alt = m.alt
    if (m.pos.toLong != 1 && _ref.head == _alt.head) {
        _ref = _ref.substring(1, _ref.length)
        _alt = _alt.substring(1, _alt.length)
        _left += 1
        _right = Math.max(_right, _left) // prefix deletion on simple insertions can generate left > right. This is a simple fix
    } else if(m.pos.toLong == 1 && _ref.last == _alt.last) {
        _ref = _ref.substring(0, _ref.length - 1)
        _alt = _alt.substring(0, _alt.length - 1)
        _right -= 1
    }
    if(_ref.length == 0)
      _ref = VCFMutation.MISSING_VALUE_CODE
    if(_alt.length == 0)
      _alt = VCFMutation.MISSING_VALUE_CODE
  }

  /**
   * Copies REF and ALT from the source mutation to _ref and _alt respectively
   */
  private def copyREF_ALT():Unit ={
    _ref = m.ref
    _alt = m.alt
  }

  /**
   * When splitting microsatellites, the resulting couples of REF and ALT alleles can contain more bases than necessary.
   * Here as an example, the mutation:
   * MT	16183	.	ACCCCCT	CCCCCCC,GCCCCCT,GCCCCCC,CCCCACT,ACCCCT	100	fa	VT=M,S,M,M,I;AC=186,7,3,1,1
   * would produce, between the others, the couple REF:ACCCCCT ALT:GCCCCCT, which is a SNP even if described with 7 bases
   * instead of just 1.
   * This method trims MNPs by removing equal bases in equal positions from REF and ALT, and consequently adjusts the
   * breakpoints and length parameters. If a trimmed MNP has both REF and ALT of length 1, its type is reduced to SNP.
   */
  private def reduceMNP(): Unit ={
    _ref = m.ref
    _alt = m.alt
    // trim common alleles to the left
    var commonAlleles = 0
    while (commonAlleles < Math.min(_ref.length, _alt.length) && _ref(commonAlleles) == _alt(commonAlleles))
      commonAlleles += 1
    _ref = _ref.substring(commonAlleles, _ref.length)
    _alt = _alt.substring(commonAlleles, _alt.length)
    _left += commonAlleles
    // trim common alleles to the right
    commonAlleles = 1
    while (commonAlleles <= Math.min(m.ref.length, m.alt.length) && m.ref(m.ref.length - commonAlleles) == m.alt(m.alt.length - commonAlleles))
      commonAlleles += 1
    _ref = m.ref.substring(0, m.ref.length - commonAlleles +1)
    _alt = m.alt.substring(0, m.alt.length - commonAlleles +1)
    _length = _ref.length
    _right = _left + _length
    // a trimmed MNP can reduce itself to a SNP
    if(_length == 1)
      _type = Some(OKGMutation.TYPE_SNP)
  }

  private def mutationStringWithoutFormat: String ={
    String.join(" ",
      m.chr, m.pos, m.id, m.ref, m.alt, m.info.toString()
    )
  }

  def length:String = {
    if(_length == -1)
      VCFMutation.MISSING_VALUE_CODE
    else
    _length.toString
  }

  def left: String ={
    _left.toString
  }

  def right:String ={
    _right.toString
  }

  def strand:String ={
    OKGMutation._strand
  }

  def mut_type: String ={
    _type.getOrElse(
      m.info.getOrElse(VCFInfoKeys.SV_TYPE,
        m.info.getOrElse(VCFInfoKeys.VARIANT_TYPE,
          VCFMutation.MISSING_VALUE_CODE))
    )
  }

  //////////////////////////////////////    TRAIT   /////////////////////////////////////

  override def chr: String = OKGMutation.CHR_PREFIX+m.chr

  override def pos: String = m.pos

  override def id: String = m.id

  override def ref: String = _ref

  override def alt: String = _alt

  override def qual: String = m.qual

  override def filter: String = m.filter

  override def info: Map[String, String] = m.info

  override def format(sampleName: String, biosamples: IndexedSeq[String]): Map[String, String] = m.format(sampleName, biosamples)
}
object OKGMutation {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  private val CHR_PREFIX = "chr"
  private val _strand:String = "+"
  val TYPE_INDEL = "INDEL"
  val TYPE_SNP = "SNP"
  val TYPE_MNP = "MNP"


  def apply(m: VCFMutationTrait): OKGMutation = {
    new OKGMutation(m)
  }


  ////////////////////////    TEST FUNCTIONS    //////////////////////////

 /*
  def TESTbreakPoints(inputFile: String, outputFile: String):Unit ={
    import it.polimi.genomics.metadata.util.vcf.MetaInformation
    MetaInformation.updatePropertiesFromMetaInformationLines(inputFile)
    val reader = FileUtil.open(inputFile).get
    val writer = FileUtil.writeReplace(outputFile).get
    advanceAndGetHeaderLine(reader)
    try {
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read and transform each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine).map(OKGMutation.apply).foreach(mutation => {
          val outputLine = formatWithDefaultAttributes(mutation)
          writer.write(outputLine)
          writer.newLine()
        })
      }, setupReadProgressCanary(inputFile))
    } finally {
      writer.close()
      reader.close()
    }
  }

  def transform(line: String): Unit ={
    VCFMutation.splitOnMultipleAlternativeMutations(line).map(OKGMutation.apply).foreach(mutation => {
      println(formatWithDefaultAttributes(mutation))
    })
  }

  def formatWithDefaultAttributes(mutation: OKGMutation): String ={
    OneKGTransformer.makeTSVString(
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

  def advanceAndGetHeaderLine(reader: BufferedReader): String = {
    var headerLine = reader.readLine()
    while (headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    headerLine
  }
  private def setupReadProgressCanary(fullFilePath: String): Option[RoughReadProgress] ={
    println("COUNTING LINES OF FILE: "+fullFilePath)
    FileUtil.countLines(fullFilePath) match {
      case Failure(_) =>
        println("COUNT OF LINES IN FILE FAILED")
        None
      case Success(value) =>
        Some(new RoughReadProgress(value, 10, RoughReadProgress.notifyProgress))
    }
  }
  */

}

