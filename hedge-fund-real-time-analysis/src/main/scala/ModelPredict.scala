import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession


object ModelPredict {

  def main(args: Array[String]): Unit = {

    //stock symbol
    val stock_symbol = "ford"

    // create spark session
    val spark = SparkSessionCreator.sparkSessionCreate()

    // train data
    val rawTestData = DataSourcer.rawTestData(sparkSession = spark, Symbol = stock_symbol)

    // clean train data
    val cleanTestData = DataCleaner.cleanData(dataFrame = rawTestData)

    // load fitted pipeline
    val fittedPipeline = PipelineModel.load("./pipelines/fitted-pipeline")

    // make predictions
    val predictions = fittedPipeline.transform(dataset = cleanTestData)

    // save prediction
    OutputSaver.predictionsSaver(sparkSession = spark, dataFrame = predictions, Symbol = stock_symbol)

  }

}
