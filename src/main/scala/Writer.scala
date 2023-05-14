import org.apache.spark.sql.DataFrame

object Writer {
  def writeOutput(df: DataFrame, outputPath: String): Unit = {
    df.write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(outputPath)
  }
}
