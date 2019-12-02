import OutputSaver._

object ModelTrain {

  def main(args: Array[String]): Unit = {

    // create spark session
    val spark = SparkSessionCreator.sparkSessionCreate()

    // train data
    val rawTrainData = DataSourcer.rawTrainData(sparkSession = spark)

    // clean train data
    val cleanTrainData = DataCleaner.cleanData(dataFrame = rawTrainData)

    // fitted pipeline
    val fittedPipeline = MachineLearning.pipelineFit(dataFrame = cleanTrainData)

    // save fitted pipeline
    pipelineSaver(pipelineModel = fittedPipeline)

  }

}
