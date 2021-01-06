package com.deltalake.hudi.comparison

import com.deltalake.hudi.comparison.DeltaLake.{DELTA_BASE_PATH, TABLE_NAME}
import com.deltalake.hudi.comparison.Hudi.HUDI_BASE_PATH
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.io.File

object DeltaLake {

  val DELTA_BASE_PATH = "C:\\deltalake\\deltalake\\data\\golden\\"
  val TABLE_NAME = "delta_table_for_comparison"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DeltaLake")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val deltalakeReaderWriter = new DeltalakeReaderWriter()

    // BOOTSTRAPING NEW TABLE WITH INSERTS
    val ds = spark.createDataFrame(deltalakeReaderWriter.createDataSet())
    ds.show()
    deltalakeReaderWriter.write(ds)

    // UPSERTS ON EXISTING DATA
    val dsForUpsert = spark.createDataFrame(deltalakeReaderWriter.createDataSetForUpsert())
    dsForUpsert.show()
    deltalakeReaderWriter.upsert(dsForUpsert)


    // UPDATE COLUMN
    deltalakeReaderWriter.update()

    //READ
    deltalakeReaderWriter.read(spark)

    deltalakeReaderWriter.generateManifests()


  }

}

class DeltalakeReaderWriter {

  val NO_OF_YEARS = 20
  val NO_OF_MONTHS = 12
  val NO_OF_ROWS_PER_PARTITION = 50000
  val NEW_NO_OF_ROWS_PER_PARTITION = 25000
  val startYear = 2000

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
      ds.write.format("delta")
        .mode("append")
        .partitionBy("year","month")
        .save(DELTA_BASE_PATH + TABLE_NAME)
      val timeTaken = (System.currentTimeMillis() - start)/1000
      println(s"Time taken to write to Delta Table : $timeTaken ")
  }

  def upsert(newData : Dataset[Row]):Unit = {
    val start = System.currentTimeMillis()
    val deltaTable = DeltaTable.forPath(DELTA_BASE_PATH + TABLE_NAME)
    deltaTable.as("oldData")
      .merge(newData.as("newData"),
        "oldData.year = newData.year and oldData.month = newData.month and oldData.pk_1 = newData.pk_1")
      .whenMatched.updateAll
      .whenNotMatched.insertAll
      .execute()
    val timeTaken = (System.currentTimeMillis() - start)/1000
    println(s"Time taken to upsert to Delta Table : $timeTaken ")
  }

  def update():Unit = {
    val start = System.currentTimeMillis()
    val deltaTable = DeltaTable.forPath(DELTA_BASE_PATH + TABLE_NAME)
    deltaTable.update(
      col("field_1") === 0.00,
      Map("field_1" -> lit(10.0))
    )
    val timeTaken = (System.currentTimeMillis() - start)/1000
    println(s"Time taken to update Delta Table : $timeTaken ")
  }

  def read(spark:SparkSession): Unit = {
    val start = System.currentTimeMillis
    val ds = spark.read.format("delta").load(DELTA_BASE_PATH + TABLE_NAME)
    ds.show
    ds.filter(ds("pk_1")==="500").show
    val timeTaken = (System.currentTimeMillis() - start)/1000
    println(s"Time taken to read from Delta Table : $timeTaken ")
  }


  def generateManifests():Unit = {
    val start = System.currentTimeMillis()
    val deltaTable = DeltaTable.forPath(DELTA_BASE_PATH + TABLE_NAME)
    deltaTable.generate("symlink_format_manifest")
    val timeTaken = (System.currentTimeMillis() - start)/1000
    println(s"Time taken to generate Manifests : $timeTaken ")
  }





}

