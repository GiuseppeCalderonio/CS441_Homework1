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
   * This function returns the content of the file 'outputPath/part-00000' specified in the parameter outputPath
   * @param outputPath the output path ideally of the map reducer job
   * @return the content of the file 'outputPath/part-00000' specified in the parameter outputPath
   */
  private def manageTestOverhead(outputPath : String): String =

    val source = Source.fromFile(outputPath + "/part-00000")

    if (source.isEmpty) {
      source.close()
      return ""
    }

    val outputLines = "\n"
      + source.getLines().toArray.reduce((l1, l2) => l1 + "\n" + l2)

    source.close()

    outputLines


  /**
   * this method is used to run the map reduce job
   * It deletes first the output directory if it exists, and then it starts the job
   * @param jobName this is the name of the job (i.e. "MyJob")
   * @param outputValueClass this is the Writable output type of the mapper class (i.e classOf(IntWritable) )
   * @param outputFormatClass this is the key value pair format of the output (i.e classOf[TextOutputFormat[Text, IntWritable])
   * @param mapperClass this is the class type of the mapper (i.e classOf[Mapper] where Mapper extends MapReduceBase)
   * @param reducerClass this is the class type of the reducer (i.e classOf[Reducer] where Reducer extends MapReduceBase)
   * @param inputPath this parameter specifies the input file of the job
   * @param outputPath this parameter specifies the output directory of the job (it should ideally be a .csv file format)
   * @param isTest this parameter specifies if the job is used for testing purposes
   * @param nMappers this parameter represents the number of mappers
   * @param nReducers this parameters represents the number of reducers
   * @return the output of the map reduce as a string if isTest is true
   */
  def runJob( jobName : String,
              outputValueClass: Class[? <: org.apache.hadoop.io.Writable],
              outputFormatClass: Class[? <: org.apache.hadoop.mapred.OutputFormat[?, ?]],
              mapperClass: Class[? <: org.apache.hadoop.mapred.Mapper[?, ?, ?, ?]],
              reducerClass: Class[? <: org.apache.hadoop.mapred.Reducer[?, ?, ?, ?]],
              inputPath : String,
              outputPath : String,
              isTest : Boolean = false,
              nMappers : String,
              nReducers : String): String =

    // delete the output path if already existing

    deleteRecursively(new File(outputPath))

    val separator = ","

    // run the job

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(jobName)
    conf.set("mapreduce.job.maps", nMappers)
    conf.set("mapreduce.job.reduces", nReducers)
    conf.set("mapreduce.output.textoutputformat.separator", separator)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(outputValueClass)
    conf.setMapperClass(mapperClass)
    conf.setCombinerClass(reducerClass)
    conf.setReducerClass(reducerClass)
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(outputFormatClass)
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))


    logger.info(s"Start executing $jobName job")

    JobClient.runJob(conf)

    logger.info(s"Job $jobName executed")

    // manage the return of the function for the test case
    if(isTest){
      manageTestOverhead(outputPath)
    }else
      ""