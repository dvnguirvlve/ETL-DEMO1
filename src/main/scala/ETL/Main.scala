package ETL

import ETL.Main.{Client_path, Produit_path, Vente_path, hdfs_path}
import ETL.engine.{Constant, Engine, READ, WRITE, csvToHdfs}
import ETL.utils.DropEmptyColumns
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit, sum, upper}

object Main extends Constant {
  def main(args : Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/hadoop")
    System.setProperty("hadoop.util.NativeCodeLoader", "false")


    val engine =new Engine()
    val spark =engine.init_spark()

    //File Client :
    val read = new READ(spark)
    val df = read.READFile(file_path = Client_path, file_format = file_format)

    //SHOW Client :
    print("CLIENT :")
    df.show()

    //SHOW SCHEMA :
    print("SCHEMA OF TABLE CLIENT :")
    df.printSchema()

    //CHANGE NAME OF COLUMNS AND FILTER EMPTY NAMES:
    val df1 = df
      .withColumnRenamed("Nom", "Full Name")
      .withColumnRenamed("adresse", "ADDRESS")
      .withColumnRenamed("ville", "PAYS")
      .filter(col("Full Name").isNotNull)

    print("AFTER CHANGING NAMES OF COLUMNS FROM FRENSH TO ENGLISH :")
    df1.show(numRows = 25)

    //SPLIT COLUMN FULL NAME AND CHANGE THE ORDER :
    val df2: DataFrame = df1.withColumn("FIRST NAME", functions.split(col("Full Name"), " ").getItem(0))
      .withColumn("LAST NAME", functions.split(col("Full Name"), " ").getItem(1))
      .drop("Full Name")
    val df3 : DataFrame = df2.select("N_Client", "FIRST NAME", "LAST NAME", "PAYS", "ADDRESS")

    print("AFTER SPLITING FULL NAME AND CHANGE THE ORDER :")
    df3.show(numRows = 30)

    // UPPER TO LAST NAMES :
    val dfFinal: DataFrame = df3.withColumn("LAST NAME", upper(col("LAST NAME")))

    print("AFTER UPPERING LAST NAMES")
    dfFinal.show(numRows = 30)


    // FILE PRODUIT :
    val read1 = new READ(spark)
    val DF = read1.READFile(file_path = Produit_path, file_format = file_format)

    //SHOW PRODUIT :
    print("PRODUIT :")
    DF.show(numRows = 30)
    print("SCHEMA :")
    DF.printSchema()

    //CHANGE NAME OF COLUMNS :
    val DF1 = DF
      .withColumnRenamed("Code_produit", "PRODUCT CODE")
      .withColumnRenamed("nom", "PRODUCT NAME")
      .withColumnRenamed("categorie", "CATEGORIE")
      .withColumnRenamed("desc_cate", "CATEGORIE DESCRIPTION")
      .withColumnRenamed("prix", "PRICE")

    //CHANGE NAMES :
    print("AFTER CHANGING NAMES :")
    DF1.show(numRows = 30)

    // Drop Empty Columns :
    val dropEmptyColumns = new DropEmptyColumns(DF1)
    val DF2 = dropEmptyColumns.dropEmptyColumns()

    print("AFTER DROPPING EMPTY COLUMNS :")
    DF2.show(numRows = 50)


    // FUSION OF TWO COLUMNS AND ORDER COLUMNS :
    val DF3 = DF2.withColumn("CATEGORIE / DESCRIPTION", functions.concat(col("CATEGORIE"), lit("/"), col("CATEGORIE DESCRIPTION")))
    val DFFinal = DF3.select("PRODUCT CODE", "PRODUCT NAME", "CATEGORIE / DESCRIPTION", "PRICE")

    print("AFTER THE FUSION AND ORDER THE COLUMNS :")
    DFFinal.show()

    /*
    print("AFTER ADDING SYMBOL $ TO PRICE :")
    val DF5 = DF4.withColumn("PRICE", functions.concat(lit("$"), format_number(col("PRICE"), 2)))
    DF5.show()
      */


    //File VENTE :
    val read2 = new READ(spark)
    val Df = read2.READFile(file_path = Vente_path, file_format = file_format)

    //SHOW VENTE :
    print("VENTE :")
    Df.show(numRows = 60)
    print("SCHEMA :")
    Df.printSchema()

    //CHANGE NAME OF COLUMNS AND ADD $ TO PRICE:
    val Df1 = Df
      .withColumnRenamed("Code_produit", "PRODUCT CODE")
      .withColumnRenamed("Quantite", "QUANTITY")
      .withColumnRenamed("prix", "PRICE")

    print("VENTE AFTER CHANGING NAMES OF COLUMNS :")
    Df1.show(numRows = 60)
    Df1.printSchema()

    val Df2 = Df1.drop("PRICE", "QUANTITY")

    /*
    val Df2 = Df1.withColumn("PRICE", functions.concat(lit("$"), format_number(col("PRICE"), 2)))
    */

    /*
    val sumValue = Df1.agg(sum("PRICE")).first()
    println(s"The sum of the 'PRICE' column is: $sumValue")
      */

    // INNER JOIN BETWEEN CLIENT AND VENTE :
    val df_Df2 = dfFinal.join(Df2, Seq("N_Client"), "inner")

    print("CLIENT INNER JOIN VENTE :")
    df_Df2.show()

    // THEN JOIN PRODUIT :
    val FINAL = df_Df2.join(DFFinal, Seq("PRODUCT CODE"), "inner")

    print("FINAL TABLE")
    FINAL.show(numRows = 100)
    FINAL.printSchema()


    /* try {
       df.write.csv("/invalid/path")
     } catch {
       case e: Exception =>
         println(s"Write failed: ${e.getMessage}")
     } */

    //Write File Client
    /*
    val write = new WRITE()
    write.WRITEFile(FINAL, output_path)
     */


    val CsvToHdfs = new csvToHdfs(spark)
    CsvToHdfs.writeCsvToHdfs(FINAL, hdfs_path)


    spark.stop()

  }
}