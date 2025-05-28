package ETL.engine

import org.apache.spark.sql.SparkSession

class Engine extends Constant {
  def init_spark(): SparkSession = {
    val spark = SparkSession.builder()
      .master(spark_master)
      .appName(app_name)
      .getOrCreate()
    spark
  }
}
