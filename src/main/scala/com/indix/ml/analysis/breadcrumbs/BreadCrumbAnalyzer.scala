package com.indix.ml.analysis.breadcrumbs

import com.indix.ml.models.TopLevelModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.parallel.CollectionsHaveToParArray

case class BreadCrumb(storeId: Int, storeName: String, doc: String, freq: Int) {
  def categorize(implicit topLevelModel: TopLevelModel) = {
    val (probVect, categoryId, category, prob) = topLevelModel.predictCategory(doc)
    BreadCrumbCategory(storeId, storeName, doc, freq, probVect.toArray, categoryId, category, prob)
  }
}

case class BreadCrumbCategory(storeId: Int, storeName: String, doc: String, freq:Int, prob: Array[Double], categoryId: Int, category: String, classProb: Double)


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
    import spark.implicits._
    val breadCrumbsDs = spark.read.json(breadCrumbsFile).as[BreadCrumb]
    breadCrumbsDs.cache().createOrReplaceTempView("store_bc")
    implicit lazy val model = TopLevelModel()
    breadCrumbsDs.rdd.map(r => r.categorize)
    spark.udf.register("tokenize", (doc: String) => model.predictProba(doc))
    val tokenDF = spark.sql("" +
      "select " +
      "   storeId,storeName, collect_set(token) as tokens from " +
      "( select storeId,storeName, explode(tokenize(breadCrumbs)) as token from store_bc) as x" +
      " group by storeId,storeName")
    tokenDF.coalesce(1).write.option("compression", "gzip").mode("overwrite").json(outputFile)
    spark.stop()
  }

}
