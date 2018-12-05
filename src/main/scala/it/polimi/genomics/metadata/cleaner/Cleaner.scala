package it.polimi.genomics.metadata.cleaner

import java.io.File

import it.polimi.genomics.metadata.step.CleanerStep.createSymbolicLink

import scala.collection.mutable.ArrayBuffer
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.channels.FileChannel
///Users/abernasconi/Documents/gitProjects/GMQL-Importer/Example/example_for_cleaner/rules.txt


object Cleaner extends App {

  if (args.length < 1) {
    println("This jar requires 2 or 3 parameters to be run: " +
      "(1) path of datasets, inside of which the application will consider:\n" +
      "- as input, directories \"Transformations\"\n " +
      "- as output, directories \"Cleaned\"\n " +
      "(2) optional parameter for file of rules to apply (otherwise Cleaned will contain symbolic links to Transformations)")
    System.exit(0)
  }

  val base_path = args(0)

  //Default case, when there exists a rule file
  var rules_file: Option[String] = None

  if(args.length > 1){
    rules_file = Some(args(1))
  }

  val ruleBasePathOpt: Option[RuleBase] = {
    if (rules_file.isDefined)
      Option(new RuleBase(rules_file.get))
    else None
  }

  val input_directory = new File(base_path + "/Transformations/")
  val output_directory = new File(base_path + "/Cleaned/")


  val input_meta_files: Array[File] = Utils.getListOfMetaFiles(input_directory)
  val input_bed_files: Array[File] = Utils.getListOfBEDFiles(input_directory)
  val input_gdm_files: Array[File] = Utils.getListOfGDMFiles(input_directory)
  val input_schema_file: Array[File] = Utils.getSchemaFile(input_directory)

  input_meta_files.foreach({ inputFile: File =>

    val inputPath = inputFile.getAbsolutePath

    val outputPath = new File(output_directory, inputFile.getName).getAbsolutePath

    //println("inputPath: " + inputPath)
    //println("outputPath: " + outputPath)

    if (ruleBasePathOpt.isDefined) {
      ruleBasePathOpt.get.applyRBToFile(inputPath, outputPath)
    } else {
      createSymbolicLink(inputPath, outputPath)
    }

  })

  input_bed_files.foreach({ inputFile: File =>

    val inputPath = inputFile.getAbsolutePath

    val outputPath = new File(output_directory, inputFile.getName).getAbsolutePath

    createSymbolicLink(inputPath, outputPath)

  })

  input_gdm_files.foreach({ inputFile: File =>

    val inputPath = inputFile.getAbsolutePath

    val outputPath = new File(output_directory, inputFile.getName).getAbsolutePath

    createSymbolicLink(inputPath, outputPath)

  })

  input_schema_file.foreach({ inputFile: File =>

    val inputPath = inputFile.getAbsolutePath

    val outputPath = new File(output_directory, inputFile.getName).getAbsolutePath

    val inputChannel: FileChannel = new FileInputStream(inputPath).getChannel
    val outputChannel: FileChannel = new FileOutputStream(outputPath).getChannel
    outputChannel.transferFrom(inputChannel, 0, inputChannel.size)
    inputChannel.close
    outputChannel.close
  })






}





