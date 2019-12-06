import com.mongodb.spark.config.ReadConfig
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.MongoSpark;



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
  def predictionsSaver(sparkSession: SparkSession , dataFrame: DataFrame): Unit = {

    dataFrame
      .select( "Timestamp","High", "Low", "Close", "Volume", "prediction")
      .write
      .option("header", "true")
      .mode(saveMode = SaveMode.Overwrite)
      .csv(path = "./predictions/predictions_csv/")

    //save in mongo
    sparkSession.conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/scaladb.fordPredict")
    //val df_predicted = sparkSession.read.csv("./predictions/predictions_csv/")
    val df_predicted = sparkSession.read.format("csv").option("header", "true").load("./predictions/predictions_csv/")

    df_predicted.write
      .option("uri","mongodb://127.0.0.1/")
      .option("spark.mongodb.output.database", "scaladb")
      .option("spark.mongodb.output.collection", "fordPrediction")

      .format(source = "mongo").mode("append").save()

  }

  def MetricSaver(dataFrame: DataFrame): Unit = {

    dataFrame
      .select("Model", "RMSE")
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .csv(path = "./Model_metrics/model_metrics_csv")


  }


}
