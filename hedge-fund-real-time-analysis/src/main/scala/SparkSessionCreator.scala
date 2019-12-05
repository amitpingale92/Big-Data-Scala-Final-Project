import org.apache.spark.sql.SparkSession

object SparkSessionCreator {

  def sparkSessionCreate(): SparkSession = {

    //    SparkSession
    //      .builder()
    //      .master("local[*]")
    //      .appName("hedge-fund-real-time-analysis")
    //      .getOrCreate()

    SparkSession
      .builder()
      .master("local")
      .appName("hedge-fund-real-time-analysis")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/scaladb.ford")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/scaladb.ford")
      .getOrCreate()

  }

}
