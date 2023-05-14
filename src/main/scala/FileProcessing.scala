import org.apache.spark.sql.functions.{col, count, first, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileProcessing {
  def processUsingAggregatesOnDf(df: DataFrame): DataFrame = {
    val dfWithProcessedMissingData = df.na.fill(0)

    dfWithProcessedMissingData.withColumn("value", when(col("value") === "", 0).otherwise(col("value")))
      .groupBy("key", "value")
      .agg(count("*").alias("count"))
      .filter(col("count") % 2 === 1)
      .groupBy("key")
      .agg(first("value").alias("value"), first("count").alias("odd_count"))
      .select("key", "value", "odd_count")
      .orderBy("key")
  }

  def processUsingPlainSQL(df: DataFrame, spark: SparkSession): DataFrame = {
    val dfWithProcessedMissingData = df.na.fill(0)
    dfWithProcessedMissingData.createOrReplaceTempView("inputData")

    val resultDF = spark.sql(
      """
       SELECT key, first(value) as value, first(count) as odd_count
       FROM (
          SELECT key, value, count(*) as count
          FROM (
            SELECT key, CASE WHEN value = '' THEN 0 ELSE value END as value
            FROM inputData
          )
          GROUP BY key, value
          HAVING count(*) % 2 = 1
       ) odd_counts
       GROUP BY key
       ORDER BY key
       """
    )

    resultDF
  }
}