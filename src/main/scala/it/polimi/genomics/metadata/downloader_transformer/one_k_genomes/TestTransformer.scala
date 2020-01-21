package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.File

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.utils.Unzipper
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.KGTransformer.{removeCopyNumber, removeExtension}
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{ApproximateReadProgress, FileUtil, PatternMatch}
import it.polimi.genomics.metadata.util.vcf.{HeaderMetaInformation, VCFInfoKeys, VCFMutation}

import scala.collection.mutable
import scala.util.Try

class TestTransformer extends Transformer{

  var numberOfCallsToGetCandidateNames = 0
  var allSamples:mutable.HashSet[String] = new mutable.HashSet[String]()

  var headerMeta:HeaderMetaInformation = null
  var mutationPrinter:MutationPrinterTrait = null

  val path22part = "D:\\tomma\\1kG random VCF\\ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf\\ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf"
  val path19part = "D:\\tomma\\1kG random VCF\\chr19_part\\ALL.chr19.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf"

  val pathX = "D:\\tomma\\1kG random VCF\\ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf\\ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf"
  val testLine1 = "X\t213981\t.\tTCCTG\tACCTG,T\t100\tPASS\tAC=2,919;AF=0.000399361,0.183506;AN=5008;NS=2504;DP=13073;AMR_AF=0,0.0994;AFR_AF=0,0.4849;EUR_AF=0.001,0.0924;SAS_AF=0.001,0.047;EAS_AF=0,0.0694;VT=SNP,INDEL;MULTI_ALLELIC\tGT\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|2\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t2|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|2\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t2|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t2|0\t2|0\t0|0\t0|0\t2|0\t0|0\t2|0\t0|0\t2|0\t2|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|0\t2|0\t2|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|2\t2|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t2|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t2|0\t0|0\t2|0\t2|2\t0|0\t0|0\t0|2\t0|0\t0|0\t2|2\t0|2\t2|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t0|0\t2|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|2\t0|2\t2|0\t2|2\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|2\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t1|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|2\t0|0\t2|2\t0|0\t0|0\t2|2\t2|0\t2|0\t2|2\t0|2\t0|0\t0|2\t2|2\t0|2\t2|2\t2|2\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|2\t0|0\t2|2\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|2\t0|2\t2|0\t0|2\t0|2\t2|0\t2|2\t2|2\t0|2\t2|2\t0|0\t0|2\t2|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|2\t2|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|2\t2|0\t0|0\t0|0\t0|2\t0|2\t2|0\t2|0\t2|0\t2|0\t0|2\t0|0\t0|2\t0|2\t0|2\t0|2\t0|2\t0|2\t0|2\t2|0\t2|2\t0|2\t2|0\t2|2\t2|0\t0|2\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t2|2\t0|2\t2|2\t0|2\t2|2\t2|0\t2|2\t2|2\t2|0\t0|0\t2|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|2\t2|0\t0|0\t2|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|2\t2|0\t2|0\t2|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|2\t2|0\t0|2\t0|2\t0|2\t0|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|2\t0|2\t0|0\t2|2\t2|0\t0|0\t0|0\t0|2\t2|2\t2|0\t0|2\t2|2\t2|0\t2|2\t2|0\t0|2\t2|0\t0|0\t0|0\t2|0\t2|0\t0|2\t0|2\t0|2\t0|2\t2|0\t0|0\t0|2\t0|2\t2|2\t2|0\t2|2\t2|0\t0|2\t2|0\t2|2\t0|2\t0|0\t2|0\t2|0\t0|2\t2|0\t2|2\t0|0\t2|0\t2|0\t2|0\t0|0\t2|2\t2|0\t0|2\t2|2\t2|2\t2|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t2|2\t0|0\t0|2\t2|2\t0|2\t0|2\t2|0\t2|0\t0|2\t0|2\t2|2\t2|0\t2|0\t2|0\t0|0\t0|0\t0|2\t2|0\t0|2\t2|0\t0|2\t0|2\t2|0\t2|2\t2|0\t2|2\t0|2\t2|2\t2|2\t2|2\t0|2\t0|0\t2|2\t0|0\t2|0\t2|0\t2|2\t0|2\t0|2\t0|2\t0|0\t2|0\t2|0\t2|0\t0|0\t2|2\t2|0\t2|0\t2|0\t2|0\t2|2\t2|0\t2|2\t0|2\t0|2\t2|2\t0|2\t2|0\t0|0\t0|2\t2|2\t2|2\t0|0\t2|2\t0|0\t2|0\t2|0\t2|0\t0|2\t0|2\t0|2\t0|2\t0|0\t2|2\t2|0\t0|2\t2|0\t0|2\t0|2\t0|0\t2|2\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|2\t2|0\t0|2\t0|0\t2|2\t2|0\t0|0\t0|0\t0|2\t2|0\t0|2\t0|2\t2|0\t2|2\t2|2\t2|2\t2|2\t0|2\t0|0\t0|0\t2|2\t2|0\t0|2\t2|0\t0|2\t0|2\t2|2\t0|2\t0|2\t2|2\t2|0\t0|2\t0|2\t2|2\t0|0\t0|2\t0|0\t0|2\t2|2\t2|0\t0|2\t2|2\t0|2\t0|0\t2|0\t2|2\t0|2\t2|2\t0|2\t0|0\t2|0\t0|0\t0|2\t2|0\t0|2\t0|2\t0|2\t0|2\t2|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t2|2\t2|0\t2|2\t2|0\t0|0\t0|0\t0|0\t0|2\t2|2\t0|0\t0|0\t2|0\t0|2\t0|2\t0|2\t2|2\t0|2\t0|0\t2|0\t0|2\t2|0\t2|0\t2|0\t2|0\t0|2\t2|2\t0|2\t0|2\t0|2\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t1|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|0\t0|2\t0|2\t0|2\t0|0\t2|0\t0|0\t2|0\t0|2\t2|0\t0|0\t2|2\t2|2\t0|2\t0|0\t0|2\t0|2\t2|0\t0|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t2|0\t2|0\t0|2\t0|2\t0|0\t2|0\t0|2\t0|2\t0|0\t2|0\t2|0\t2|2\t0|0\t2|2\t0|0\t0|2\t0|2\t2|2\t0|0\t2|0\t0|0\t0|2\t0|0\t2|2\t2|0\t0|2\t2|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t2|0\t2|2\t0|2\t2|2\t2|2\t2|2\t0|2\t2|0\t2|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|2\t2|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t2|2\t2|2\t2|0\t2|0\t0|2\t2|2\t2|2\t0|2\t0|2\t2|0\t2|0\t2|2\t0|0\t2|0\t0|2\t0|2\t0|0\t0|2\t0|2\t2|0\t2|2\t2|0\t2|0\t0|0\t2|0\t0|2\t0|2\t0|0\t0|0\t2|0\t2|0\t0|0\t2|0\t2|2\t2|2\t0|0\t0|0\t2|0\t0|2\t2|2\t0|0\t2|2\t2|0\t2|2\t2|0\t2|0\t2|0\t0|0\t0|2\t0|2\t2|0\t2|0\t0|2\t2|0\t0|2\t2|2\t0|2\t2|0\t2|0\t2|0\t0|0\t2|2\t0|2\t0|0\t0|0\t0|0\t0|2\t2|0\t0|2\t0|0\t2|0\t0|2\t2|0\t0|0\t0|2\t0|0\t2|0\t2|2\t2|2\t0|2\t0|2\t2|2\t2|0\t0|2\t2|0\t0|0\t0|0\t0|0\t2|0\t0|2\t0|0\t2|0\t2|0\t0|0\t0|2\t2|0\t0|2\t0|2\t0|2\t2|2\t2|0\t2|2\t2|0\t2|2\t0|0\t0|2\t0|2\t2|0\t0|0\t2|2\t0|2\t2|0\t2|0\t2|2\t0|2\t0|0\t0|2\t2|2\t2|2\t0|0\t0|2\t2|2\t0|2\t0|0\t2|0\t0|0\t2|2\t0|2\t0|0\t2|2\t0|2\t2|2\t0|0\t2|0\t0|0\t0|2\t0|2\t0|0\t2|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|2\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t2|0\t0|0\t0|2\t2|0\t2|0\t0|0\t0|0\t0|2\t2|2\t2|0\t0|0\t0|0\t2|0\t2|0\t2|0\t0|0\t2|0\t0|2\t0|2\t0|2\t0|2\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|2\t0|2\t0|0\t2|0\t0|2\t0|2\t0|0\t0|2\t0|0\t2|2\t2|0\t0|2\t2|0\t2|0\t2|2\t2|2\t2|0\t0|0\t0|0\t2|2\t0|0\t0|2\t2|2\t2|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|2\t0|0\t2|0\t0|0\t0|2\t2|0\t2|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t2|0\t0|0\t0|2\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t2|2\t0|0\t2|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|2\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t2|2\t0|0\t0|0\t0|0\t0|0\t0|2\t0|2\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|0\t0|2\t0|0\t0|0\t0|0\t2|0\t0|0\t0|0\t0|0\t0|0"

  val pathMT = "C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager\\Example\\examples_meta\\1kGenomes\\hg19\\Downloads\\ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf"
  val corrected_pathMT = "C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager\\Example\\examples_meta\\1kGenomes\\hg19\\Downloads\\corrected_ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf"
  val testLine2 = "MT\t453\t.\tTTT\tATT,T,CTT\t100\tfa\tVT=S,I,S;AC=1,1,1\tGT\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t2\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t3\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
  val testLine3 = "MT\t750\t.\tAA\tGA,GG\t100\tfa\tVT=S,M;AC=2503,1\tGT\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t0\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t0\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t2\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1"

  private var mutationEnricher: KGMutation => KGMutation = (m:KGMutation) => m

  def enrichMutationsBeforeWriting(f: KGMutation => KGMutation): Unit ={
    mutationEnricher = f
  }

  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val targetFileName = filename
    val targetFilePath = destinationPath+File.separator+targetFileName
    val transformationsDirPath = destinationPath+File.separator

    if(targetFileName.endsWith("gdm")){
//      transformX()
//      transformMT()
//      new VCFAdapter(path19part, mutationPrinter).appendAllMutations(targetFilePath)
//      new VCFAdapter(path22part, mutationPrinter).appendAllMutations(targetFilePath)
//      new VCFAdapter(pathX, mutationPrinter).appendAllMutations(targetFilePath)
//      new VCFAdapter(corrected_pathMT, mutationPrinter).appendAllMutations(targetFilePath)

      new VCFAdapter(corrected_pathMT, mutationPrinter)
//        .enrichMutationsBeforeWriting((m:KGMutation) => m
//        .addInfoAttribute(VCFInfoKeys.NUMBER_OF_SAMPLES, "2534")
//        .addInfoAttribute(VCFInfoKeys.NUMBER_OF_ALLELES, "2534")
//        .addInfoAttribute(VCFInfoKeys.ALLELE_FREQUENCY, (m.info(VCFInfoKeys.ALLELE_COUNT).toFloat/2534).toString))
        .appendAllMutations(targetFilePath)
//      findMultiAllelicAncestralAllele(path19part)
//      FileUtil.createReplaceEmptyFile(targetFilePath)
    } else
      FileUtil.createReplaceEmptyFile(targetFilePath)
    true
  }

  def transformMT(): Unit ={
    val VCFPath = fixVCF_chrMT(pathMT)
    headerMeta = new HeaderMetaInformation(VCFPath)
    println("TEST LINE 2")
    println(testLine2)
    VCFMutation.splitOnMultipleAlternativeMutations(testLine2, headerMeta).map(KGMutation.apply).foreach(mutation => {
      val outputLine = mutationPrinter.formatMutation(mutation)
      println(outputLine)
    })
    println("TEST LINE 3")
    println(testLine3)
    VCFMutation.splitOnMultipleAlternativeMutations(testLine3, headerMeta).map(KGMutation.apply).foreach(mutation => {
      val outputLine = mutationPrinter.formatMutation(mutation)
      println(outputLine)
    })
  }

  def transformX(): Unit ={
    headerMeta = new HeaderMetaInformation(pathX)
    val reader = FileUtil.open(pathX).get    // let it throw exception if failed
    var headerLine = reader.readLine()
    while(headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    FileUtil.scanFileAndClose(reader, mutationLine => {
      VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).foreach(mutation => {
        val ancestralAllele = mutation.info.get(VCFInfoKeys.ANCESTRAL_ALLELE)
        if(ancestralAllele.isDefined && ancestralAllele.get.contains(",")) {
          println(mutationPrinter.formatMutation(mutation))
          println("press enter to continue")
          scala.io.StdIn.readLine()
        }
      })
    })
  }

  def findMultiAllelicAncestralAllele(VCFFilePath:String): Unit ={
    val headerMeta = new HeaderMetaInformation(VCFFilePath)
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    var headerLine = reader.readLine()
    while(headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    FileUtil.scanFileAndClose(reader, mutationLine => {
      VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).foreach(mutation => {
        val ancestralAllele = mutation.info.get(VCFInfoKeys.ANCESTRAL_ALLELE)
        if(ancestralAllele.isDefined && (/*ancestralAllele.get.contains(",") ||*/ mutation.info.get(VCFInfoKeys.SV_TYPE).isDefined)) {
          println(mutationPrinter.formatMutation(mutation))
          println("press enter to continue")
          scala.io.StdIn.readLine()
        }
      })
    }, setupReadProgressCanary(VCFFilePath))
  }

  def testMutationEnricher(VCFFilePath:String): Unit ={
    val headerMeta = new HeaderMetaInformation(VCFFilePath)
    val reader = FileUtil.open(VCFFilePath).get    // let it throw exception if failed
    var headerLine = reader.readLine()
    while(headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    FileUtil.scanFileAndClose(reader, mutationLine => {
      VCFMutation.splitOnMultipleAlternativeMutations(mutationLine, headerMeta).map(KGMutation.apply).map(mutationEnricher)
        .foreach(mutation => {
          val AF = mutation.info.get(VCFInfoKeys.NUMBER_OF_SAMPLES)
          val AC = mutation.info.get(VCFInfoKeys.NUMBER_OF_ALLELES)
          if(AC.isEmpty || AF.isEmpty){
            println("attribute enriched not found for muta "+mutationPrinter.formatMutation(mutation))
            println("press enter to continue")
            scala.io.StdIn.readLine()
          }
      })
    }, setupReadProgressCanary(VCFFilePath))
  }



  /**
   * Returns the path of a new VCF file equal to the original except for:
   *  - the cardinality of the INFO attribute AC is changed from "." to "A" in the meta-information section because the
   *  cardinality "." represents the relaxation of "A" which doesn't allow to correctly separate the value of AC in
   *  microsatellite mutations. The correctness of the new cardinality has been checked manually on the VCF file for
   *  chromosome MT on 18 Jan 2020.
   * @param VCFFilePath the path of the VCF file to correct.
   * @return the path path of the corrected VCF.
   */
  protected def fixVCF_chrMT(VCFFilePath: String): String ={
    // create new file
    val newFilename = "corrected_"+FileUtil.getFileNameFromPath(VCFFilePath)
    val containingDir = FileUtil.getContainingDirFromFilePath(VCFFilePath)
    val newPath = containingDir+newFilename
    val writer = FileUtil.writeReplace(newPath).get
    // prepare regex
    val regexPattern = PatternMatch.createPattern("##INFO=<ID=AC,Number=\\.,(.*)")  // matches only the attributes cardinality undefined
    // find & correct the wrong declaration of AC
    val reader = FileUtil.open(VCFFilePath).get
    var originalLine = reader.readLine()
    var partsOfString = PatternMatch.matchParts(originalLine, regexPattern)
    while(partsOfString.isEmpty && originalLine!=null){
      writer.write(originalLine)
      writer.newLine()
      originalLine = reader.readLine()
      partsOfString = PatternMatch.matchParts(originalLine, regexPattern)
    }
    writer.write("##INFO=<ID=AC,Number=A," + partsOfString.head)
    writer.newLine()
    // copy the rest of the file
    FileUtil.scanFileAndClose(reader, line => {
      writer.write(line)
      writer.newLine()
    })
    newPath
  }

  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
    if(numberOfCallsToGetCandidateNames == 0){
      mutationPrinter = SchemaAdapter.fromSchema(source.rootOutputFolder+File.separator+dataset.schemaUrl, dataset)
      fixVCF_chrMT(pathMT)
      numberOfCallsToGetCandidateNames += 1
      List("a.gdm")
    } else if(numberOfCallsToGetCandidateNames == 1) {
      numberOfCallsToGetCandidateNames += 1
      List("a.gdm.meta")
    } else
      List.empty[String]
  }

//  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
//    val downloadDirPath = dataset.fullDatasetOutputFolder+File.separator+"Downloads"+File.separator
//    val trueFilename = removeCopyNumber(filename)
//    if(trueFilename.contains("chrY") || trueFilename.contains("chrMT")){
//      println("current sample set size "+allSamples.size)
//      extractArchive(downloadDirPath+trueFilename, downloadDirPath+removeExtension(trueFilename)) match {
//        case None => List.empty[String]
//        case Some(_VCFPath) =>
//          // read sample names and output resulting filenames:
//          val samples = new VCFAdapter(_VCFPath).biosamples.map(sample => s"$sample.gdm").toList
//          println(""+samples.size+" samples in file "+trueFilename)
//          samples.foreach(sample => allSamples.add(sample))
//          println("new sample set size "+allSamples.size)
//      }
//      List("a.gdm")
//    } else if(trueFilename.contains("sequence"))
//      List("a.gdm.meta")
//    else
//      List.empty[String]
//  }

  private def setupReadProgressCanary(fullFilePath: String): Option[ApproximateReadProgress] ={
    val shortName = Try(FileUtil.getFileNameFromPath(fullFilePath).split("\\.")(1)).getOrElse(FileUtil.getFileNameFromPath(fullFilePath))
    println("COUNTING LINES OF FILE " + shortName)
    val numberOfLinesInFile = FileUtil.countLines(fullFilePath).toOption
    println("COUNTED "+numberOfLinesInFile.get+" LINES IN "+shortName)
    Some(new ApproximateReadProgress(
      numberOfLinesInFile.get,
      progressNotificationStep = 10,
      ApproximateReadProgress.simpleProgressNotification(filename = shortName)))

  }

  protected def extractArchive(archivePath: String, extractedFilePath: String):Option[String] ={
      println("EXTRACTING "+archivePath.substring(archivePath.lastIndexOf(File.separator)))
      if (Unzipper.unGzipIt(archivePath, extractedFilePath)) {
        Some(extractedFilePath)
      } else {
        println("EXTRACTION FAILED. THE PACKAGE IS PROBABLY DAMAGED OR INCOMPLETE. " +
          "SKIP TRANSFORMATION FOR THIS PACKAGE.")
        None
      }
  }

}
