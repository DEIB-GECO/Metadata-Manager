package it.polimi.genomics.metadata.util.vcf

/**
 * Created by Tom on ott, 2019
 */
abstract class VCFMutationTrait {

  def chr:String
  def pos:String
  def id:String
  def ref:String
  def alt:String
  def qual:String
  def filter:String
  def info: Map[String, String]
  def format(sampleName: String, biosamples: IndexedSeq[String]): Map[String, String]
  def isSampleMutated(formatOfSample: Map[String, String]): Boolean
  /**
   * Tells you which chromosome copy is mutated
   */
  def mutatedChromosomeCopy(formatOfSample: Map[String, String]): Seq[String]
}
