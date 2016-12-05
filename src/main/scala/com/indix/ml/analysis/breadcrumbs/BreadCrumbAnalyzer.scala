package com.indix.ml.analysis.breadcrumbs

import breeze.linalg.DenseVector
import com.indix.ml.models.TopLevelModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

case class BreadCrumb(storeId: Int, storeName: String, doc: String, freq: Int) {
  def categorize(implicit topLevelModel: TopLevelModel) = {
    val prob = topLevelModel.predictProba(doc)
    BreadCrumbCategory(storeId, storeName, freq, prob.toArray)
  }
}

case class BreadCrumbCategory(storeId: Int, storeName: String, freq: Int, prob: Array[Double]) {

  def weightedProb: DenseVector[Double] = DenseVector(prob: _*) * freq.toDouble

  def +(other: BreadCrumbCategory) = {
    require(storeId == other.storeId)
    val prob: DenseVector[Double] = weightedProb + other.weightedProb
    BreadCrumbCategory(storeId, storeName, freq + other.freq, prob.toArray)
  }

  def normalize = (DenseVector(prob) / freq.toDouble).toArray
}


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
    val breadCrumbCategory = breadCrumbsDs.rdd.map(r => r.categorize)
    val byStore: RDD[(Int, BreadCrumbCategory)] = breadCrumbCategory.keyBy(x => x.storeId)
    val storeWiseProbs = byStore.reduceByKey(_ + _).values
    val storeCategories = breadCrumbCategory.map(x => model.predictCategory(DenseVector(x.prob)))
    storeCategories.keyBy(x => (x._1, x._2))
    //    probDf.coalesce(1).write.option("compression", "gzip").mode("overwrite").json(outputFile)
    spark.stop()
  }

}
