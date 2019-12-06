import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import OutputSaver._


import scala.math._
import org.apache.spark.sql.types._
import java.util.HashMap
import java.util.Map;

// do final data preparations for machine learning here
// define and run machine learning models here
// this should generally return trained machine learning models and or labelled data
// NOTE this may be merged with feature engineering to create a single pipeline

object MachineLearning {



  def pipelineFit(dataFrame: DataFrame): PipelineModel = {
    println("*************************************")
    dataFrame.show()
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

    // ************************* compute test error *************************
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // ************************* define Linear regression *************************
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(df_train)
    val predictions_lr = lrModel.transform(df_test)

    // select (prediction, true label)
    predictions_lr.select("prediction", "label", "features").show(5)

    val rmse_lr = evaluator.evaluate(predictions_lr)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_lr")

    // ************************* define decision tree regression *************************
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val dtModel = dt.fit(df_train)
    val predictions_dt = dtModel.transform(df_test)

    predictions_dt.select("prediction", "label", "features").show(5)

    val rmse_dt = evaluator.evaluate(predictions_dt)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_dt")


    // ************************* define random forest regression *************************
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val rfrModel = rf.fit(df_train)
    val predictions_rfr = rfrModel.transform(df_test)

    // select (prediction, true label)
    predictions_rfr.select("prediction", "label", "features").show(5)

    // compute test error

    val rmse_rfr = evaluator.evaluate(predictions_rfr)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_rfr")

    // ************************* define Gradient boosting regression *************************
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    val gbtModel = gbt.fit(df_train)
    val predictions_gbt = gbtModel.transform(df_test)
    predictions_gbt.select("prediction", "label", "features").show(5)

    val rmse_gbr = evaluator.evaluate(predictions_gbt)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_gbr")

    //*************************  DataFrame for analysing model metrics *************************
    val ss = SparkSession.builder().appName("DataSet Test")
      .master("local[*]").getOrCreate()
    import ss.implicits._

    val ModelMetricDS = Seq(
      ("Linear Regressor", rmse_lr),
      ("Decision Tree Regressor", rmse_dt),
      ("Random Forest Regressor", rmse_rfr),
      ("Gradient Boosting Regressor", rmse_gbr)).toDF("Model", "RMSE")

    ModelMetricDS.show()
    OutputSaver.MetricSaver(ModelMetricDS)


    //*************************  Selecting model with lowest RSME value for deployment in pipeline *************************
//    val models = Map(
//      lrModel -> rmse_lr,
//      dtModel -> rmse_dt,
//      rfrModel -> rmse_rfr,
//      gbtModel -> rmse_gbr
//    )

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
