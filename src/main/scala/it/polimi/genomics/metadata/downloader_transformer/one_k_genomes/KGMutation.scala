package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import it.polimi.genomics.metadata.util.vcf.{VCFInfoKeys, VCFMutation, VCFMutationTrait}
import org.slf4j.{Logger, LoggerFactory}

/**
 * This class extends the general VCFMutationTrait with methods and attributes whose implementation is specific for 1000Genomes,
 * since they are not mandatory VCF attributes and so their definition depends on the custom attributes specified in the file
 * by each source.
 *
 * This class adds to VCFMutationTrait the attributes:
 * - strand
 * - left and right breakpoints computed as 0-based coordinates
 * - length: it is right - left except on insertions, where the length is the size of the inserted sequence.
 * - type: aggregates values coming from VT and SVTYPE attributes. Plus, it uniforms CNV loss with DEL type and
 * discriminates insertions from deletions in indels variants.
 *
 * Additionally, it shrink mutations by removing the padding base usually present in indels and SV with symbolic alternative
 * alleles.
 */
class KGMutation(m: VCFMutationTrait) extends VCFMutationTrait {

  private var _ref:String = _
  private var _alt:String =  _
  private var _length: Long = -1
  private var _left: Long = -1
  private var _right: Long = -1
  private var _type: Option[String] = None

  /**
   * This constructor computes the breakpoints for this mutation as 0-based coordinates. Also, it initializes the
   * attributes _type, length and _ref/ _alt removing the common prefix base ("shrinking operation") because it is
   * useless when using 0-based coordinates.
   * At the end of this method, _length may not be initialized due to incomplete information in the original mutation;
   * in such a case, when a user requires the length,it will receive the string NULL.
   */
  {
    import VCFInfoKeys._
    // VCF specifies in POS the 1-based coordinate of the base in REF. -1 is added for converting it into a 0-based coordinate.
    _left = m.pos.toLong -1
    if(m.info.get(SV_TYPE).isDefined) {   // SVs:
      // For SVs, "the VCF record specifies a-REF-t and the alternative haplotypes are a-ALT-t for each alternative allele"
      if (m.info.get(SV_END).isDefined) {
        _right = m.info(SV_END).toLong    // END is equivalent in both 1-based and 0-based coordinates.
        _length =  _right - _left
      } else if(m.info.get(SV_LENGTH).isDefined) {
        /* From VCF 4.0, SVLEN can be a positive or a negative value (when the SV represents a long deletion).
        * For precise variants, SVLEN = END - POS + 1, which in 0-based coordinates is equivalent to _right - _left.
        * For imprecise variants SVLEN is an estimation, so it can be greater than END - POS + 1. */
        _length = m.info(SV_LENGTH).toLong.abs
      }
      shrinkSV()
      // compute _right if it's missing
      if(_right == -1) {
        if(m.alt.startsWith(KGMutation.SV_INS_ALT_PREFIX))
          _right = _left
        else if(_length != -1 && m.info.get(CONFID_INTERVAL_AROUND_END_POS).isEmpty){
          VCFMutation.logger.info("CHECK THIS OUT "+mutationStringWithoutFormat)
          _right = _left + _length
        }
      }
      // type
      if(m.alt.equals(KGMutation.CN0))
        _type = Some(KGMutation.TYPE_SV_DEL)
      else
        _type = m.info.get(VCFInfoKeys.SV_TYPE).orElse(m.info.get(VCFInfoKeys.VARIANT_TYPE))  // can be None
    } else {
      // THE VARIANT TYPE ATTRIBUTE HAS CARDINALITY UNDEFINED IN VCFs, SO IT ISN'T SPLIT AS THE MUTATIONS. DO NOT TRUST ITS VALUE
      // INDELs
      if (m.info.get(SV_TYPE).isEmpty && m.ref.length != m.alt.length ) {
        // In INDELS, ALT and REF always share the first or the last base.
        _length = Math.max(m.ref.length, m.alt.length) -1
        if(m.alt.length < m.ref.length) { // deletion
          _right = _left + m.ref.length
          _type = Some(KGMutation.TYPE_DEL)
        } else {  // insertion
          _right = _left
          _type = Some(KGMutation.TYPE_INS)
        }
        shrinkINDEL()
      } // SNPs
      else if (m.info.get(SV_TYPE).isEmpty && m.ref.length == 1 && m.alt.length == 1) {
        _type = Some(KGMutation.TYPE_SNP)
        _length = 1
        _right = _left + 1
        copyREF_ALT()
      } // MNPs (a SNP like "TTT -> ATT" falls into this case but during shrinking it collapses to "T->A" and the type is corrected)
      else if (m.info.get(SV_TYPE).isEmpty && m.ref.length == m.alt.length ) {
        _type = Some(KGMutation.TYPE_MNP)
        // MNP mutations 're like concatenated SNPs
        // skip _length and _right initialization because they're then overwritten in shrinkMNP()
        shrinkMNP()
      }
    }
    // fallback case
    if( _length == -1 || _right == -1) {
      KGMutation.logger.info(s"UNHANDLED VARIANT: $mutationStringWithoutFormat \n left: ${_left} length: ${_length} right: ${_right}")
      if(_right == -1)
        _right = _left
      if(_ref == null || _alt == null)
        copyREF_ALT()
    }
  }

  /**
   * In SVs, normally "the VCF record specifies a-REF-t and the alternative haplotypes are a-ALT-t for each alternative
   * allele". However "if any of the ALT alleles is a symbolic allele (an angle-bracketed ID String "< ID>") then the
   * padding base is required and POS denotes the coordinate of the base preceding the polymorphism.".
   * In this method we initialize _ref, _alt and eventually we remove the padding base adjusting the coordinates consequently.
   */
  private def shrinkSV():Unit ={
    _ref = m.ref.drop(1)
    _left += 1
    if(_length != -1)
      _length -= 1
    _alt = if(!m.alt.startsWith(KGMutation.SYMBOLIC_MUTATION_ALT_PREFIX)) {
      // log this kind of mutations because they're rare and I want to check the correctness of the algorithm for those cases too.
      KGMutation.logger.debug("CHECK NON-SYMBOLIC SV: "+mutationStringWithoutFormat)
      m.alt.drop(1)
    } else m.alt
    if(_ref.length == 0)
      _ref = VCFMutation.MISSING_VALUE_CODE
    if(_alt.length == 0)
      _alt = VCFMutation.MISSING_VALUE_CODE
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
  private def shrinkINDEL():Unit ={
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
  private def shrinkMNP(): Unit ={
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
      _type = Some(KGMutation.TYPE_SNP)
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

  val strand:String = "+"

  def mut_type: String ={
    _type.getOrElse(
      m.info.getOrElse(VCFInfoKeys.SV_TYPE,
        m.info.getOrElse(VCFInfoKeys.VARIANT_TYPE,
          VCFMutation.MISSING_VALUE_CODE))
    )
  }

  def addInfoAttribute(key:String, value:String): KGMutation = {
    info += key -> value
    this
  }

  //////////////////////////////////////    TRAIT   /////////////////////////////////////

  override def chr: String = KGMutation.CHR_PREFIX+m.chr

  override def pos: String = m.pos

  override def id: String = m.id

  override def ref: String = _ref

  override def alt: String = _alt

  override def qual: String = m.qual

  override def filter: String = m.filter

  var info: Map[String, String] = m.info

  override def format(sampleName: String, biosamples: IndexedSeq[String]): Map[String, String] = m.format(sampleName, biosamples)

  override def isSampleMutated(formatOfSample: Map[String, String]): Boolean = m.isSampleMutated(formatOfSample)

  override def mutatedChromosomeCopy(formatOfSample: Map[String, String]): Seq[String] = m.mutatedChromosomeCopy(formatOfSample)
}
object KGMutation {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  private val CHR_PREFIX = "chr"
  private val TYPE_DEL = "DEL"
  private val TYPE_INS = "INS"
  private val TYPE_SNP = "SNP"
  private val TYPE_MNP = "MNP"
  private val TYPE_SV_DEL = "DEL"
  private val CN0 = "<CN0>"
  private val SV_INS_ALT_PREFIX = "<INS"
  private val SYMBOLIC_MUTATION_ALT_PREFIX = "<"


  def apply(m: VCFMutationTrait): KGMutation = {
    new KGMutation(m)
  }


  ////////////////////////    TEST FUNCTIONS    //////////////////////////

 /*
  def TESTbreakPoints(inputFile: String, outputFile: String):Unit ={
    import it.polimi.genomics.metadata.util.vcf.HeaderMetaInformation
    HeaderMetaInformation.updatePropertiesFromMetaInformationLines(inputFile)
    val reader = FileUtil.open(inputFile).get
    val writer = FileUtil.writeReplace(outputFile).get
    advanceAndGetHeaderLine(reader)
    try {
      FileUtil.scanFileAndClose(reader, mutationLine => {
        // read and transform each mutation
        VCFMutation.splitOnMultipleAlternativeMutations(mutationLine).map(KGMutation.apply).foreach(mutation => {
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
    VCFMutation.splitOnMultipleAlternativeMutations(line).map(KGMutation.apply).foreach(mutation => {
      println(formatWithDefaultAttributes(mutation))
    })
  }

  def formatWithDefaultAttributes(mutation: KGMutation): String ={
    OKGTransformer.makeTSVString(
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
  */

}

