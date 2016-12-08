package com.indix.ml.analysis.breadcrumbs

import breeze.linalg.DenseVector
import com.indix.ml.models.TopLevelModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class BreadCrumb(storeId: Long, storeName: String, breadCrumbs: String, noItems: Long) {
  @transient lazy val logger = Logger.getLogger(this.getClass.getName)

  def categorize(topLevelModel: TopLevelModel) = {
    logger.info("before predict")
    val (sparsity, probability): (Double, DenseVector[Double]) = topLevelModel.predictProba(breadCrumbs)
    logger.info("after predict")
    val prob = probability * noItems.toDouble
    logger.info("after scaling")
    val weightedSparsity = sparsity * noItems.toDouble
    logger.debug(s"categorizing $storeId")
    BreadCrumbCategory(storeId, storeName, noItems, prob.toArray, weightedSparsity)
  }

}

case class BreadCrumbCategory(storeId: Long, storeName: String, noItems: Long, weightedProb: Array[Double], tokenSparsity: Double) {

  def +(other: BreadCrumbCategory) = {
    require(storeId == other.storeId)
    val probSum: DenseVector[Double] = DenseVector(other.weightedProb: _*) + DenseVector(weightedProb: _*)
    BreadCrumbCategory(storeId, storeName, noItems + other.noItems, probSum.toArray, tokenSparsity + tokenSparsity)
  }

  def category(topLevelModel: TopLevelModel) = {
    val (categoryId, category, probability) = topLevelModel.predictCategory(DenseVector(weightedProb: _*))
    CategoryPrediction(storeId, storeName, categoryId, category, probability, noItems, tokenSparsity)
  }

  def normalize = (DenseVector(weightedProb) / noItems.toDouble).toArray
}

case class CategoryPrediction(storeId: Long, storeName: String, categoryId: Long, category: String, weightedProb: Double, noItems: Long, tokenSparsity: Double) {
  def prob = weightedProb / noItems
}


object BreadCrumbAnalyzer {
  def main(args: Array[String]) {
    @transient lazy val logger = Logger.getLogger(this.getClass.getName)
    logger.setLevel(Level.INFO)
    val inputFile = args(0)
    val storeWiseCategoryProbabilities = args(1)
    val storeWiseCategoryPredictions = args(2)
    val breadCrumbsFile = inputFile
    logger.info(s"The inputfile is $inputFile")
    logger.info(s"The output files are $storeWiseCategoryProbabilities and $storeWiseCategoryPredictions")
    val spark = SparkSession.builder().appName("BreadCrumbAnalyzer").getOrCreate()
    import spark.implicits._
    val breadCrumbsDs = spark.read.json(breadCrumbsFile).as[BreadCrumb].cache()
    implicit lazy val model = TopLevelModel()
    breadCrumbsDs.show()

    /*    Compute the weighted average of store wise probabilities weighted by the breadcrumb frequency*/
    val fields = Seq(
      StructField("storeId", LongType, nullable = false),
      StructField("storeName", StringType, nullable = false),
      StructField("noItems", LongType, nullable = false)
    ) ++ (0 until model.categoryIds.size).map(x => StructField("p_" + model.categoryIds(x), DoubleType, nullable = false))
    val schema = StructType(fields)
    val breadCrumbCategory = breadCrumbsDs.rdd.mapPartitions(iterator => {
      val model = TopLevelModel()
      iterator.map(r =>
        r.categorize(model)
      )
    }).filter(x => x.tokenSparsity > 0.0)
    val byStore: RDD[(Long, BreadCrumbCategory)] = breadCrumbCategory.keyBy(x => x.storeId)
    val categoryProbabilities: RDD[BreadCrumbCategory] = byStore.reduceByKey(_ + _).values
    val storeWiseProbs = categoryProbabilities.map(r => Row.fromSeq(Seq(r.storeId, r.storeName, r.noItems) ++ r.normalize))
    val storeWiseProbsDF = spark.createDataFrame(storeWiseProbs, schema)
    storeWiseProbsDF.show()
    storeWiseProbsDF.coalesce(1).write.mode("overwrite").json(storeWiseCategoryProbabilities)

    /*    Compute the store wise weighted aggregate of category predicted for each breadcrumb, weighted by breadcrumb frequency*/
    val categoryForBreadCrumb: Dataset[CategoryPrediction] = breadCrumbCategory.mapPartitions(iterator => {
      val model = TopLevelModel()
      iterator.map( x => {
         x.category(model)
      })
    }).toDS()

    val categoryDF = categoryForBreadCrumb.groupBy("storeId", "storeName", "categoryId", "category")
      .agg(expr("sum(weightedProb)/sum(noItems) as probability").as[Double],
        expr("sum(tokenSparsity)/sum(noItems) as sparsity").as[Double]).filter("sparsity > 0.0")
    categoryDF.show()
    categoryDF.coalesce(1).write.mode("overwrite").json(storeWiseCategoryPredictions)
    spark.stop()
  }

}
