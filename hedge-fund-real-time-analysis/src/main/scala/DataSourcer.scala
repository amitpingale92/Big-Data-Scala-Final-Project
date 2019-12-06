import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark

import org.apache.spark.sql.functions.{max, min}
import org.bson.Document

object DataSourcer {

  def rawTrainData(sparkSession: SparkSession, Symbol : String): DataFrame = {

    // load train data from local
    sparkSession.read.option("header", "true").csv("./src/main/resources/F.csv") // mongoDB all documents with particular stock
//    val readConfig: ReadConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "scaladb", "collection" ->  Symbol)) // 1)
//    sparkSession.read.mongo(readConfig)


  }

  def rawTestData(sparkSession: SparkSession, Symbol : String): DataFrame = {

    // load train data from local
    // populate label col - not included in raw test data
    //    sparkSession.read.option("header", "true").csv("./src/main/resources/test.csv") //mongoDB 1st document of document
    //      .withColumn("Open", lit("0"))

      //    sparkSession.read.option("header", "true").csv("./src/main/resources/F-test.csv") //mongoDB 1st document of document
      //      .withColumn("Open", lit("0"))

        val readConfig: ReadConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "scaladb", "collection" -> Symbol)) // 1)
        val df = sparkSession.read.mongo(readConfig)
        //val df: DataFrame = sparkSession.read.format("mongo").load()
        df.createOrReplaceTempView("data")
        sparkSession.sql("Select timestamp,open,high,low,close,volume FROM data order by Timestamp desc limit 1")

  }

}
