import org.apache.spark.ml.PipelineModel

object ModelPredict {

  def main(args: Array[String]): Unit = {

    // create spark session
    val spark = SparkSessionCreator.sparkSessionCreate()

    // train data
    val rawTestData = DataSourcer.rawTestData(sparkSession = spark)

    // clean train data
    val cleanTestData = DataCleaner.cleanData(dataFrame = rawTestData)

    // load fitted pipeline
    val fittedPipeline = PipelineModel.load("./pipelines/fitted-pipeline")

    // make predictions
    val predictions = fittedPipeline.transform(dataset = cleanTestData)

    // save prediction
    OutputSaver.predictionsSaver(dataFrame = predictions)

  }

}
