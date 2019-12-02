import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SaveMode}
// save your output from the various objects here

object OutputSaver {

  // function to save a fitted pipeline
  def pipelineSaver(pipelineModel: PipelineModel): Unit = {

    pipelineModel
      .write
      .overwrite()
      .save("./pipelines/fitted-pipeline")

  }

  // function to save predictions
  def predictionsSaver(dataFrame: DataFrame): Unit = {

    dataFrame
      .select("High", "Low", "Close", "Volume", "prediction")
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .csv(path = "./predictions/predictions_csv")

  }

}
