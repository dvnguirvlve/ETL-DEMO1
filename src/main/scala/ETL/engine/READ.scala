package ETL.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

class READ(spark : SparkSession) extends Constant {
  def READFile(file_path: String, file_format: String): DataFrame = {
    val df = spark.read
      .format(file_format)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .load(file_path)
    df
  }
}
