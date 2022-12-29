package com.quiz

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

object QuizReport extends Serializable {
  def fetchAnswer(colName:String) = {
    when(col(colName+".1").isNotNull,lit(1))
      .when(col(colName+".2").isNotNull,lit(2))
      .when(col(colName+".3").isNotNull,lit(3))
      .when(col(colName+".4").isNotNull,lit(4))
      .when(col(colName+".5").isNotNull,lit(5))
      .otherwise(null)
  }
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName("extract_report").getOrCreate()

    val jsonInputData = spark.read.json("C:\\reports\\cbse_quiz\\2022-01-28-0-1643323054539.json.gz")
    val filteredJsonInputData = jsonInputData.filter(jsonInputData("eid")==="ASSESS")
    val inputDF = filteredJsonInputData.selectExpr("actor.id as ActorId", "eid as EventId", "edata.item.title as Question", "edata.resvalues[0] as AnswerOptions", "object.rollup.l1 as CourseId","object.id as ObjectId")
    val outputDF = inputDF.withColumn("Answer",fetchAnswer("AnswerOptions")).drop("AnswerOptions")
    outputDF.repartition(1).write.mode("overwrite").option("header", "true").csv("C:\\reports\\2022-01-28-0-1643323054539")

    spark.stop()
  }
}
