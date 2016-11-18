package com.indix.ml.analysis.breadcrumbs

import org.apache.spark.sql.SparkSession


object BreadCrumbAnalyzer {
  def main(args: Array[String]) {
    val breadCrumbsFile = "/home/vumaasha/Downloads/StoreWiseBreadCrumbStats.json"
    val spark = SparkSession.builder().appName("BreadCrumbAnalyzer").getOrCreate()
    val breadCrumbsDs = spark.read.json(breadCrumbsFile)
    breadCrumbsDs.createOrReplaceTempView("store_bc")
    spark.udf.register("tokenize", (doc: String) => WordNetTokenizer.tokenize(doc))
    val tokenDF = spark.sql("select distinct storeId,token from ( select storeId,explode(tokenize(breadCrumbs)) as token from store_bc) as x limit 10")
    tokenDF.show(false)
    spark.stop()
  }

}
