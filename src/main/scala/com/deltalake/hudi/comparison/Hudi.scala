package com.deltalake.hudi.comparison

import com.deltalake.hudi.comparison.DeltaLake.DELTA_BASE_PATH
import com.deltalake.hudi.comparison.Hudi.{HUDI_BASE_PATH, TABLE_NAME}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.util
import java.util.Arrays

object Hudi {

  val HUDI_BASE_PATH = "C:\\Work\\CA\\deltalake\\hudi\\data\\"
  val TABLE_NAME = "hudi_table_for_comparison"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Hudi")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val hudiReaderWriter = new HudiReaderWriter()


    // BOOTSTRAPING NEW TABLE WITH INSERTS
    val ds = spark.createDataFrame(hudiReaderWriter.createDataSet())
    ds.show
    hudiReaderWriter.write(ds)

    // UPSERTS ON EXISTING DATA
    val dsForUpsert = spark.createDataFrame(hudiReaderWriter.createDataSetForUpsert())
    dsForUpsert.show()
    hudiReaderWriter.write(dsForUpsert)


    // READ TABLE
    hudiReaderWriter.read(spark)


  }

}

class HudiReaderWriter {

  val NO_OF_YEARS = 5
  val NO_OF_MONTHS = 1
  val NO_OF_ROWS_PER_PARTITION = 50
  val NEW_NO_OF_ROWS_PER_PARTITION = 25000
  val startYear = 2000

  val HUDI_SCHEMA = StructType(
    List(
      StructField("year", IntegerType, true),
      StructField("month", IntegerType, true),
      StructField("pk_1",StringType),
      StructField("field_1",DoubleType),
      StructField("field_2",DoubleType),
      StructField("field_3",DoubleType),
      StructField("field_4",DoubleType),
      StructField("field_5",DoubleType),
      StructField("field_6",DoubleType),
      StructField("field_7",DoubleType),
      StructField("field_8",DoubleType),
      StructField("field_9",DoubleType),
      StructField("create_date",TimestampType)
    )
  )

  def createDataSet():Seq[DatasetForTesting] = {
    val datasetForTestingList  = for  {
      year <- startYear to (startYear + NO_OF_YEARS)
      month <- 1 to NO_OF_MONTHS
      row <- 1 to NO_OF_ROWS_PER_PARTITION
    } yield DatasetForTesting(year,month,""+row)
    datasetForTestingList
  }

  def createDataSetForUpsert():Seq[DatasetForTesting] = {
    val datasetForTestingList  =  { for  {
      year <- startYear to (startYear + NO_OF_YEARS)
      month <- 1 to NO_OF_MONTHS
      row <- 2 until NO_OF_ROWS_PER_PARTITION by 2
    } yield DatasetForTesting(year,month,""+row,10.0) } ++  { for {
      year <- startYear to (startYear + NO_OF_YEARS)
      month <- 1 to NO_OF_MONTHS
      row <- (NO_OF_ROWS_PER_PARTITION + 1) to (NO_OF_ROWS_PER_PARTITION + NEW_NO_OF_ROWS_PER_PARTITION)
    } yield DatasetForTesting(year,month,""+row) }
    datasetForTestingList

  }

  def write(ds : Dataset[Row]):Unit = {
    val start = System.currentTimeMillis()
    ds.write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY,"pk_1")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY,"year,month")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY,"org.apache.hudi.keygen.ComplexKeyGenerator")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY,"create_date")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY,"upsert")
      .option(HoodieWriteConfig.TABLE_NAME,TABLE_NAME)

      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY,"jdbc:hive2://<HIVE_SRVR>:<PORT>/<DATABASE>;transportMode=http;httpPath=cliservice;AuthMech=3;UID=<USER_NAME>;PWD=<PASSWORD>")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY,"true")
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY,"<DATABASE>")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,"HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY")
      .mode(SaveMode.Append)
      .save(HUDI_BASE_PATH + TABLE_NAME)
    val timeTaken = (System.currentTimeMillis() - start)/1000
    println(s"Time taken to write to Hudi Table : $timeTaken ")

  }

  def read(spark:SparkSession): Unit = {
    val start = System.currentTimeMillis
    var ds = spark.read.format("hudi").schema(HUDI_SCHEMA).load(HUDI_BASE_PATH + TABLE_NAME + "/*/*/*")
    ds.filter(ds("pk_1")==="500").show
    val timeTaken = (System.currentTimeMillis() - start)/1000
    println(s"Time taken to read from Hudi Table : $timeTaken ")
  }

}


