import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.reflect.io.Directory

class FileProcessingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val testCSVInputPath = "src/test/resources/inputCSV"
  private val testTSVInputPath = "src/test/resources/inputTSV"
  private val testOutputPath = "src/test/resources/output"
  private val directory = new Directory(Paths.get(testOutputPath).toFile)

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("FileProcessingTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  val initialSchema: StructType = StructType(Seq(
    StructField("key", IntegerType, nullable = true),
    StructField("value", IntegerType, nullable = true)
  ))

  val expectedSchema: StructType = StructType(Seq(
    StructField("key", IntegerType, nullable = true),
    StructField("value", IntegerType, nullable = true),
    StructField("odd_count", IntegerType, nullable = true)
  ))

  "FileProcessing" should "return a correct schema using aggregates on DF for CSV data" in {
    val spark = this.spark
    val df = Receiver.readData(testCSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, testOutputPath)

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val expectedDf = spark.createDataFrame(expectedData).toDF(expectedSchema.fieldNames: _*)
    val actualDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)

    val expectedDfSchema = expectedDf.schema.map(f => StructField(f.name, f.dataType, nullable = true))
    val actualDfSchema = actualDf.schema.map(f => StructField(f.name, f.dataType, f.nullable))

    actualDfSchema shouldEqual expectedDfSchema

    directory.deleteRecursively()
  }

  "FileProcessing" should "return a correct schema using aggregates on DF for TSV data" in {
    val spark = this.spark
    val df = Receiver.readData(testTSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, testOutputPath)

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val expectedDf = spark.createDataFrame(expectedData).toDF(expectedSchema.fieldNames: _*)
    val actualDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)

    val expectedDfSchema = expectedDf.schema.map(f => StructField(f.name, f.dataType, nullable = true))
    val actualDfSchema = actualDf.schema.map(f => StructField(f.name, f.dataType, f.nullable))

    actualDfSchema shouldEqual expectedDfSchema

    directory.deleteRecursively()
  }

  "FileProcessing" should "return a correct schema using plain SQL on DF" in {
    val spark = this.spark
    val df = Receiver.readData(testCSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, testOutputPath)

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val expectedDf = spark.createDataFrame(expectedData).toDF(expectedSchema.fieldNames: _*)
    val actualDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)

    val expectedDfSchema = expectedDf.schema.map(f => StructField(f.name, f.dataType, nullable = true))
    val actualDfSchema = actualDf.schema.map(f => StructField(f.name, f.dataType, f.nullable))

    actualDfSchema shouldEqual expectedDfSchema

    directory.deleteRecursively()
  }

  "FileProcessing" should "process data correctly and write a correct output using plain SQL for CSV input data" in {
    val spark = this.spark
    val df = Receiver.readData(testCSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingPlainSQL(df, spark)
    Writer.writeOutput(processedDf, testOutputPath)

    def dfToSeq(df: DataFrame): Seq[(Int, Int, Int)] = {
      val rows = df.collect()
      rows.map(row => (row.getInt(0), row.getInt(1), row.getInt(2)))
    }

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val outputDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)
    val actualData = dfToSeq(outputDf)

    actualData shouldEqual expectedData

    directory.deleteRecursively()
  }

  "FileProcessing" should "process data correctly and write a correct output using plain SQL for TSV input data" in {
    val spark = this.spark
    val df = Receiver.readData(testTSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingPlainSQL(df, spark)
    Writer.writeOutput(processedDf, testOutputPath)

    def dfToSeq(df: DataFrame): Seq[(Int, Int, Int)] = {
      val rows = df.collect()
      rows.map(row => (row.getInt(0), row.getInt(1), row.getInt(2)))
    }

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val outputDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)
    val actualData = dfToSeq(outputDf)

    actualData shouldEqual expectedData

    directory.deleteRecursively()
  }

  "FileProcessing" should "process data correctly and write a correct output using aggregates on DF for CSV data" in {
    val spark = this.spark
    val df = Receiver.readData(testCSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, testOutputPath)

    def dfToSeq(df: DataFrame): Seq[(Int, Int, Int)] = {
      val rows = df.collect()
      rows.map(row => (row.getInt(0), row.getInt(1), row.getInt(2)))
    }

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val outputDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)
    val actualData = dfToSeq(outputDf)

    actualData shouldEqual expectedData

    directory.deleteRecursively()
  }

  "FileProcessing" should "process data correctly and write a correct output using aggregates on DF for TSV data" in {
    val spark = this.spark
    val df = Receiver.readData(testTSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, testOutputPath)

    def dfToSeq(df: DataFrame): Seq[(Int, Int, Int)] = {
      val rows = df.collect()
      rows.map(row => (row.getInt(0), row.getInt(1), row.getInt(2)))
    }

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val outputDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)
    val actualData = dfToSeq(outputDf)

    actualData shouldEqual expectedData

    directory.deleteRecursively()
  }

  "FileProcessing" should "replace null values with 0 using plain SQL" in {
    val spark = this.spark
    val df = Receiver.readData(testCSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingPlainSQL(df, spark)
    Writer.writeOutput(processedDf, testOutputPath)

    def dfToSeq(df: DataFrame): Seq[(Int, Int, Int)] = {
      val rows = df.collect()
      rows.map(row => (row.getInt(0), row.getInt(1), row.getInt(2)))
    }

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val outputDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)
    val actualData = dfToSeq(outputDf)

    actualData shouldEqual expectedData

    directory.deleteRecursively()
  }

  "FileProcessing" should "replace null values with 0 using aggregates on DF" in {
    val spark = this.spark
    val df = Receiver.readData(testCSVInputPath, spark, initialSchema)
    val processedDf = FileProcessing.processUsingAggregatesOnDf(df)
    Writer.writeOutput(processedDf, testOutputPath)

    def dfToSeq(df: DataFrame): Seq[(Int, Int, Int)] = {
      val rows = df.collect()
      rows.map(row => (row.getInt(0), row.getInt(1), row.getInt(2)))
    }

    val expectedData = Seq(
      (1, 0, 1),
      (3, 4, 3)
    )

    val outputDf: DataFrame = Receiver.readData(testOutputPath, spark, expectedSchema)
    val actualData = dfToSeq(outputDf)

    actualData shouldEqual expectedData

    directory.deleteRecursively()
  }
}