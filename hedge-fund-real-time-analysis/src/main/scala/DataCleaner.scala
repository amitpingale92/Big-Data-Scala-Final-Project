import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{mean, round}

// clean raw data frames here - format columns, replace missing values etc.

object DataCleaner {

  // function to produce a clean data frame from a raw data frame
  def cleanData(dataFrame: DataFrame): DataFrame = {

    // def function to format data correctly
    def formatData(dataFrame: DataFrame): DataFrame = {

      dataFrame
        .withColumn("Timestamp", dataFrame("Timestamp").cast("String"))
        .withColumn("Open", dataFrame("Open").cast("Double"))
        .withColumn("High", dataFrame("High").cast("Double"))
        .withColumn("Low", dataFrame("Low").cast("Double"))
        .withColumn("Close", dataFrame("Close").cast("Double"))
        .withColumn("Volume", dataFrame("Volume").cast("Double"))
        .withColumnRenamed("Open", "label")

    }

    // format raw data
    val outputData = formatData(dataFrame)


    // return cleaned data frame
    outputData

  }

}
