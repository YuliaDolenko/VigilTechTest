import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val awsKeys = args(2)

    val spark = SparkSession.builder
      .appName("FileProcessing")
      .config("spark.hadoop.fs.s3a.access.key", sys.env(awsKeys + "_ACCESS_KEY"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env(awsKeys + "_SECRET_KEY"))
      .master("local[*]")
      .getOrCreate()

    val schema: StructType = StructType(Seq(
      StructField("key", IntegerType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    ))

    val df = Receiver.readData(inputPath, spark, schema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, outputPath)
  }
}
