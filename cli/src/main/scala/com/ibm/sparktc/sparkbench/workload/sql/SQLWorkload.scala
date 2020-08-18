/**
  * (C) Copyright IBM Corp. 2015 - 2017
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package com.ibm.sparktc.sparkbench.workload.sql

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.{Formats, SaveModes}
import com.ibm.sparktc.sparkbench.utils.SparkFuncs._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SQLWorkloadResult(
                              name: String,
                              timestamp: Long,
                              loadTime: Long,
                              queryTime: Long,
                              saveTime: Long = 0L,
                              limit: Int,
                              output: Boolean,
                              total_Runtime: Long
                            )

object SQLWorkload extends WorkloadDefaults {
  val name = "sql"
  def apply(m: Map[String, Any]) =
    new SQLWorkload(
      input = m.get("input").map(_.asInstanceOf[String]),
      queryID = m.get("queryid").map(_.asInstanceOf[String]),
      inputs = m.get("inputs").map(_.asInstanceOf[String]),
      output = m.get("output").map(_.asInstanceOf[String]),
      saveMode = getOrDefault[String](m, "save-mode", SaveModes.error),
      queryStr = getOrThrow(m, "query").asInstanceOf[String],
      cache = getOrDefault[Boolean](m, "cache", false),
      numPartitions = m.get("partitions").map(_.asInstanceOf[Int]),
      limit = m.get("limit").map(_.asInstanceOf[Int])
    )

}

case class SQLWorkload (input: Option[String],
                        queryID: Option[String] = None,
                        inputs: Option[String] = None,
                        output: Option[String] = None,
                        saveMode: String,
                        queryStr: String,
                        cache: Boolean,
                        numPartitions: Option[Int] = None,
                        limit: Option[Int] = None
                       ) extends Workload {

  def loadFromDisk(spark: SparkSession): (Long, DataFrame) = time {
    val df = load(spark, input.get)
    if(cache) df.cache()
    df
  }

  def loadFromDisk(spark: SparkSession, inputPath: String): DataFrame = {
    val df = load(spark, inputPath, Option("parquet"))
    if(cache) {
      df.cache()
      df.foreach(_ => ())
    }
    df
  }

  def query(spark: SparkSession): (Long, DataFrame) = time {
    val res = spark.sqlContext.sql(queryStr)

    val partitionedRes = numPartitions match {
      case Some(value) => res.repartition(value)
      case _ => res
    }
    limit match {
      case Some(value) => partitionedRes.limit(value)
      case _ => partitionedRes
    }
  }

  def loop(res: DataFrame): (Long, Unit) = time {
    res.foreach(_ => ())
  }

  def save(res: DataFrame, where: String, spark: SparkSession): (Long, Unit) = time {
    writeToDisk(where, saveMode, res, spark)
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    if (inputs.isDefined) {
      val tables = inputs.get.split(",")
        .map(_.split("@"))
        .map(values => (values(0), values(1)))
        .map { case (name, inputPath) => (name, loadFromDisk(spark, inputPath)) }

      tables
        .foreach {
          case (tableName, df) => df.createOrReplaceTempView(tableName)
        }
    }

    val loadtime = 0L

    if (input.isDefined) {
      val (_, inputDf) = loadFromDisk(spark)
      inputDf.createOrReplaceTempView("input")
    }

    val timestamp = System.currentTimeMillis()

    val (querytime, res) = query(spark)
    val (savetime, _) = output match {
      case Some(dir) => save(res, dir, spark)
      case _ => loop(res)
    }
    val total = loadtime + querytime + savetime

    spark.createDataFrame(Seq(
      SQLWorkloadResult(
        "sql",
        timestamp,
        loadtime,
        querytime,
        savetime,
        limit.getOrElse(-1),
        output.isDefined,
        total
      )
    ))
  }

}