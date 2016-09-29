package com.spotify.spark.bigquery

import java.lang.{String => jString}
import java.util.{ArrayList => jArray}

import com.google.api.services.bigquery.model.{DatasetReference, TableList, TableSchema}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConversions._

class BigQueryPy {

  var sqlc: SQLContext = null

  def init(sqlc: SQLContext, projectId: jString): Unit = {
    this.sqlc = sqlc
    sqlc.setBigQueryProjectId(projectId)
  }

  def init(sqlc: SQLContext): Unit =
    init(sqlc, "xpn-analytics-notebook-1")

  def select(sql: jString): DataFrame =
    sqlc.bigQuerySelect(sql)

  def selectAsTable(sql: jString, tableName: jString): DataFrame =
    sqlc.bigQuerySelect(sql, tableName)

  def listDatasets(projectId: jString): jArray[DatasetReference] =
    new jArray[DatasetReference](sqlc.bq.listDatasets(projectId))

  def listTables(projectId: jString, datasetId: jString): jArray[TableList.Tables] =
    new jArray[TableList.Tables](sqlc.bq.listTables(projectId, datasetId))

  def getSchema(tableId: jString): TableSchema =
    sqlc.bq.getSchema(tableId)

  def saveDataFrame(tableSpec: jString, df: DataFrame): Unit =
    df.saveAsBigQueryTable(tableSpec)

  def deleteTable(tableSpec: jString): Unit =
    sqlc.bq.deleteTable(tableSpec)

}