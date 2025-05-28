package ETL.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

class csvToHdfs(spark: SparkSession) {
  def writeCsvToHdfs(df: DataFrame, hdfs_path: String): Unit = {
    df.write
      .option("header", "true")
      .mode("Overwrite")
      .csv(hdfs_path)
  }

}