package com.indix.ml.analysis.breadcrumbs

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}

object BreadCrumbAnalyzer {
  def main(args: Array[String]) {
    val logger = Logger.getLogger(this.getClass.getName)
    logger.setLevel(Level.INFO)
    val inputFile = args(0)
    val outputFile = args(1)
    val breadCrumbsFile = inputFile
    logger.info(s"The inputfile is $inputFile")
    logger.info(s"The output file is $outputFile")
    val spark = SparkSession.builder().appName("BreadCrumbAnalyzer").getOrCreate()
    val breadCrumbsDs = spark.read.json(breadCrumbsFile)
    breadCrumbsDs.repartition(3000).cache().createOrReplaceTempView("store_bc")
    spark.udf.register("tokenize", (doc: String) => WordNetTokenizer.tokenize(doc))
    val tokenDF = spark.sql("" +
      "select " +
      "   storeId,storeName, collect_set(token) as tokens from " +
      "( select storeId,storeName, explode(tokenize(breadCrumbs)) as token from store_bc) as x" +
      " group by storeId,storeName")
    tokenDF.coalesce(1).write.option("compression", "gzip").mode("overwrite").json(outputFile)
    spark.stop()
  }

}
