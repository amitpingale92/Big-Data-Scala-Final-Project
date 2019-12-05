import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object DataSourcer {

  def rawTrainData(sparkSession: SparkSession): DataFrame = {

    // load train data from local
    //sparkSession.read.option("header", "true").csv("./src/main/resources/data.csv") // mongoDB all documents with particular stock
    sparkSession.read.format("mongo").load()
  }

  def rawTestData(sparkSession: SparkSession): DataFrame = {

    // load train data from local
    // populate label col - not included in raw test data
    //    sparkSession.read.option("header", "true").csv("./src/main/resources/test.csv") //mongoDB 1st document of document
    //      .withColumn("Open", lit("0"))

    sparkSession.read.option("header", "true").csv("./src/main/resources/test-1.csv") //mongoDB 1st document of document
      .withColumn("Open", lit("0"))

  }

}
