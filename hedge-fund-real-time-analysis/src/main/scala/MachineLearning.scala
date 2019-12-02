import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.DataFrame
import java.util.HashMap;
import java.util.Map;

// do final data preparations for machine learning here
// define and run machine learning models here
// this should generally return trained machine learning models and or labelled data
// NOTE this may be merged with feature engineering to create a single pipeline

object MachineLearning {



  def pipelineFit(dataFrame: DataFrame): PipelineModel = {

    // define feature vector assembler
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array[String](
        "High",
        "Low",
        "Close",
        "Volume"
        )
      )
      .setOutputCol("features")

    val Array(trainingData, testData) = dataFrame.randomSplit(Array(0.7, 0.3))

    val df_train = featureAssembler.transform(trainingData)
    val df_test =  featureAssembler.transform(testData)

    //val final_model

    // ************************* define random forest regression *************************
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val rfrModel = rf.fit(df_train)
    val predictions = rfrModel.transform(df_test)

    // select (prediction, true label)
    predictions.select("prediction", "label", "features").show(5)

    // compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse_rf = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_rf")


    //Selecting model with lowest RSME value for deployment in pipeline
    val final_model = rf


    // chain indexers and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(
        Array(
          featureAssembler,
          final_model
        )
      )


    // fit pipeline
    val pipelineModel = pipeline.fit(dataFrame)

    // return fitted pipeline
    pipelineModel

  }

}
