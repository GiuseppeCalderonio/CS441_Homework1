package MapReducers

import HelperUtils.HelperFunctions.{createDirectory, deleteRecursively}
import HelperUtils.{CreateLogger, Parameters}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.*
import org.apache.log4j.BasicConfigurator

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import scala.io.Source

/**
 * this object is used to run the map reduce job
 */
object MapReducerJob :

  /**
   * this value is used for logging purposes
   */
  private val logger = CreateLogger(classOf[Parameters.type])

  /**
   * this method is used to run the map reduce job
   * It deletes first the output directory if it exists, and then it starts the job
   * @param jobName this is the name of the job (i.e. "MyJob")
   * @param outputValueClass this is the Writable output type of the mapper class (i.e classOf(IntWritable) )
   * @param outputFormatClass this is the key value pair format of the output (i.e classOf[TextOutputFormat[Text, IntWritable])
   * @param mapperClass this is the class type of the mapper (i.e classOf[Mapper] where Mapper extends MapReduceBase)
   * @param reducerClass this is the class type of the reducer (i.e classOf[Reducer] where Reducer extends MapReduceBase)
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
              inputPath : String,
              outputPath : String,
              firstLine : String = "",
              tempOutputPath : String = "default",
              localFileSystem : String = "file:///"): String =

    // define the temporary file in which the output of the job will be stored
    val outputTempDir = Parameters.tempDir + tempOutputPath

    deleteRecursively(new File(outputTempDir))

    val separator = ","

    // run the job

    val conf: JobConf = new JobConf(this.getClass)
    conf.setUser("Yarn")
    conf.setJobName(jobName)
    conf.set("fs.defaultFS", localFileSystem)
    System.setProperty( "hadoop.home.dir", "D:\\hadoop-3.3.2\\hadoop-3.3.2")
    conf.set("mapreduce.job.maps", Parameters.nMappers)
    conf.set("mapreduce.job.reduces", Parameters.nReducers)
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


    logger.info(s"Start executing $jobName job")
    JobClient.runJob(conf)



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

      deleteRecursively(new File(outputPath))

      val pw = new PrintWriter(new File(outputPath))
      pw.write(outputLines)
      pw.close()
    }

    logger.info(s"Job $jobName executed")

    outputLines


