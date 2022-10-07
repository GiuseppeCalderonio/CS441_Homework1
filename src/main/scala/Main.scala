import HelperUtils.HelperFunctions.createDirectory
import HelperUtils.{CreateLogger, Parameters}

import java.io.{File, FileNotFoundException}

/**
 * This object is the main of the program
 * in particular, it executes all the jobs
 */
object Main :


  /**
   * main method, and then it runs the four jobs
   * ASSUMPTION : the inputPath and the outputPath are correctly set
   * @param inputPath this is the input path of the map reducers, it should contain the input file
   * @param outputPath this is the output directory of the map reducers, it it does not exist the program creates it
   */
  @main def run(inputPath: String, outputPath: String): Unit =

    // run the jobs

    MapReducers.StatisticalMapReducer.run(inputPath, outputPath)
    MapReducers.ErrorTimeIntervalsMapReducer.run(inputPath, outputPath)
    MapReducers.TypeCounterMapReducer.run(inputPath, outputPath)
    MapReducers.MaxCharCountMapReduce.run(inputPath, outputPath)

  end run



