import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object Receiver {
  def readData(inputPath: String, spark: SparkSession, schema: StructType): DataFrame = {
    val dfTSV = spark.read
      .option("header", "true")
      .schema(schema)
      .option("delimiter", "\t")
      .csv(inputPath)

    val dfCSV = spark.read
      .option("header", "true")
      .schema(schema)
      .option("delimiter", ",")
      .csv(inputPath)

    val areAllTSVRowsNull: Boolean = dfTSV.filter(row => row.isNullAt(0)).count() == dfTSV.count()
    val areAllCSVRowsNull: Boolean = dfCSV.filter(row => row.isNullAt(0)).count() == dfCSV.count()

    var correctDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    if (!areAllTSVRowsNull) {
      correctDf = dfTSV
    } else if (!areAllCSVRowsNull) {
      correctDf = dfCSV
    }

    correctDf
  }
}
