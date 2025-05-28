package ETL.engine

trait Constant {
  val spark_master : String = "local[*]"
  val app_name : String = "ETL_DEMO"
  val file_format : String = "csv"
  val Client_path : String = "D:\\Demo Files\\Client.csv"
  val Produit_path : String = "D:\\Demo Files\\Produit.csv"
  val Vente_path : String = "D:\\Demo Files\\Vente.csv"
  val output_path : String = "C:\\OUTPUT"
  val hdfs_path : String = "C:\\hadoop\\OUTPUT"
}
