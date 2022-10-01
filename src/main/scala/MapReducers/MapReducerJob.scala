package MapReducers

import HelperUtils.Parameters
import org.apache.log4j.BasicConfigurator
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, RunningJob, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import scala.io.Source

/**
 * this object is used to run the map reduce job
 */
object MapReducerJob :


  /**
   * This method deletes the directory "file" recursively
   *
   * @param file the file / directory to delete recursively
   */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  def createDirectory(file : String): Unit =

    val x = new File(file)
    x.mkdir()

  /**
   * this method is used to run the map reduce job
   * It deletes first the output directory if it exists, and then it starts the job
   * @param jobName this is the name of the job (i.e. "MyJob")
   * @param outputValueClass this is the Writable output type of the mapper class (i.e classOf(IntWritable) )
   * @param outputFormatClass this is the key value pair format of the output (i.e classOf[TextOutputFormat[Text, IntWritable])
   * @param mapperClass this is the class type of the mapper (i.e classOf[Mapper] where Mapper extends MapReduceBase)
   * @param reducerClass this is the class type of the reducer (i.e classOf[Reducer] where Reducer extends MapReduceBase)
   * @param nMappers this parameter specifies te number of mappers
   * @param nReducers this parameter specifies the number of reducers
   * @param inputPath this parameter specifies the input file of the job
   * @param outputPath this parameter specifies the output directory of the job (it should ideally be a .csv file)
   * @param firstLine this parameter specifies the first line of the .csv file
   * @param tempOutputPath this parameters specifies the temporary directory of the job inside the " Parameters.tempDir" directory
   * @return the output of the map reduce as a string
   */
  def runJob( jobName : String,
              outputValueClass: Class[? <: org.apache.hadoop.io.Writable],
              outputFormatClass: Class[? <: org.apache.hadoop.mapred.OutputFormat[?, ?]],
              mapperClass: Class[? <: org.apache.hadoop.mapred.Mapper[?, ?, ?, ?]],
              reducerClass: Class[? <: org.apache.hadoop.mapred.Reducer[?, ?, ?, ?]],
              nMappers : Int = 1,
              nReducers : Int = 1,
              inputPath : String = Parameters.inputPath,
              outputPath : String = Parameters.outputPath,
              firstLine : String = "",
              tempOutputPath : String = "default"): String =



    // define the output directory
    val outputDirectory = Parameters.outputDirectory

    // if the output directory does not exist, create it

    createDirectory(outputDirectory)

    // define the temporary file in which the output of the job will be stored
    val outputTempDir = Parameters.tempDir + tempOutputPath

    deleteRecursively(new File(outputTempDir))

    val separator = ","

    // run the job

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(jobName)
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.job.maps", nMappers.toString)
    conf.set("mapreduce.job.reduces", nReducers.toString)
    conf.set("mapreduce.output.textoutputformat.separator", separator)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(outputValueClass)
    conf.setMapperClass(mapperClass)
    conf.setCombinerClass(reducerClass)
    conf.setReducerClass(reducerClass)
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(outputFormatClass)
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputTempDir))


    try{
      JobClient.runJob(conf)

    } catch {
          // if something goes wrong
      case _ : IOException =>
        System.err.println("Error, Job failed, check log for more info")
        return ""
    }


    // read the content of the temporary file just written

    val source = Source.fromFile(outputTempDir + "/part-00000")

    if(source.isEmpty) {
      source.close()
      return ""
    }

    val outputLines = firstLine + "\n"
      + source.getLines().toArray.reduce( (l1, l2) => l1 + "\n" + l2 )

    source.close()

    if(!firstLine.equals("")) {

      // write the result in the output path as a out.csv file

      deleteRecursively(new File(outputDirectory + outputPath))

      val pw = new PrintWriter(new File(outputDirectory + outputPath))
      pw.write(outputLines)
      pw.close()
    }

    outputLines


