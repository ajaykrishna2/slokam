package com.merge

import org.apache.spark.sql.SparkSession

object MergeReport extends Serializable {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName("merge_report").getOrCreate()
    val userInfoDF = spark.read.option("header", true).csv("C:\\reports\\0134766018946826241_userinfo_20220823.csv").na.fill("")
    userInfoDF.printSchema()
    val progressDF = spark.read.option("header", true).csv("C:\\reports\\0134766018946826241_progress_20220823.csv").na.fill("")
    progressDF.printSchema()

    val commonColnames = List("Collection Id","Collection Name","Batch Id","Batch Name","User UUID","State","District", "Org Name","School Id","School Name","User Type","User Sub Type")

    val mergeDF = userInfoDF.join(progressDF,commonColnames).select("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "User Name", "Email ID", "Mobile Number", "Consent Provided", "Consent Provided Date", "Org Name", "Declared Board", "User Type", "User Sub Type", "State", "District", "Block Name", "Cluster Name", "School Id", "School Name", "Enrolment Date", "Completion Date", "Certificate Status", "Progress", "do_21347654821997772818 - Score")

    mergeDF.repartition(1).write.mode("overwrite").option("header", "true").csv("C:\\reports\\0134766018946826241_merge_20220823")

    spark.stop()
  }
}
