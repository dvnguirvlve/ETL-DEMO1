package ETL.engine

import org.apache.spark.sql.{DataFrame, SaveMode}

class WRITE extends Constant {
  def WRITEFile(df: DataFrame, output_path: String, mode: SaveMode = SaveMode.Overwrite) ={
    df.write
      .mode("Overwrite")
      .option("header", "true")
      .csv(output_path)
  }
}