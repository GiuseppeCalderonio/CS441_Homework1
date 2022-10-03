import HelperUtils.HelperFunctions.createDirectory
import HelperUtils.{CreateLogger, Parameters}

import java.io.{File, FileNotFoundException}

/**
 * This object is the main of the program
 * in particular, it executes all the jobs
 */
object Main :

  /**
   * this value is used for logging purposes
   */
  private val logger = CreateLogger(classOf[Main.type])

  /**
   * main method, verifies if the configuration parameters are correctly set, and then it runs the four jobs
   * @param inputPath this is the input path of the map reducers, it should contain the input file
   * @param outputPath this is the output directory of the map reducers, it it does not exist the program creates it
   */
  @main def run(inputPath: String, outputPath: String): Unit =


    // check if the input and output file actually exists
    /*
    if !new File(inputPath).exists()
      then {
      logger.error("Input file does not exist"); throw new FileNotFoundException(s"file $inputPath not found")
    }

    if !new File(outputPath).exists()
      then {
      logger.warn("Output directory does not exist, it will be created")
      createDirectory(outputPath)
    }
  */
    // run the jobs

    MapReducers.StatisticalMapReducer.run(inputPath, outputPath)
    MapReducers.ErrorTimeIntervalsMapReducer.run(inputPath, outputPath)
    MapReducers.TypeCounterMapReducer.run(inputPath, outputPath)
    MapReducers.MaxCharCountMapReduce.run(inputPath, outputPath)

  end run



