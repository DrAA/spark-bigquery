package com.spotify.spark.bigquery

import java.lang.{String => jString}
import java.util.{ArrayList => jArray}

import com.google.api.services.bigquery.model.{DatasetReference, TableList, TableSchema}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.prop.Configuration

import scala.collection.JavaConversions._

class BigQueryPy {

  var session: SparkSession = null
  var client: BigQueryClient = null

  def init(session: SparkSession): Unit = {
    this.session = session
    val conf = session.sparkContext.hadoopConfiguration
    client = new BigQueryClient(conf)
  }

  def select(sql: jString): DataFrame =
    session.sqlContext.bigQuerySelect(sql)

  def selectAsTable(sql: jString, tableName: jString): DataFrame =
    session.sqlContext.bigQuerySelect(sql, tableName)

  def listDatasets(projectId: jString): jArray[DatasetReference] =
    new jArray[DatasetReference](session.sqlContext.bq.listDatasets(projectId))

  def listTables(projectId: jString, datasetId: jString): jArray[TableList.Tables] =
    new jArray[TableList.Tables](session.sqlContext.bq.listTables(projectId, datasetId))

  def getSchema(tableId: jString): TableSchema =
    session.sqlContext.bq.getSchema(tableId)

  def saveDataFrame(tableSpec: jString, df: DataFrame): Unit =
    df.saveAsBigQueryTable(tableSpec)

  def deleteTable(tableSpec: jString): Unit =
    session.sqlContext.bq.deleteTable(tableSpec)

}