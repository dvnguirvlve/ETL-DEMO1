package ETL.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DropEmptyColumns(df: DataFrame) {
  def dropEmptyColumns(): DataFrame = {
    // Identify columns that are not completely null
    val nonEmptyColumns = df.columns.filter(colName => {
      df.filter(col(colName).isNotNull).count() > 0
    })

    // Select only non-empty columns
    df.select(nonEmptyColumns.map(col): _*)
  }
}
