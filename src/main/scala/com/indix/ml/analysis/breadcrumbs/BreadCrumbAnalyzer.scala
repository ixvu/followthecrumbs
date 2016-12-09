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
    logger.debug("start categorize")
    val (sparsity, probability): (Double, DenseVector[Double]) = topLevelModel.predictProba(breadCrumbs)
    val prob = probability * noItems.toDouble
    val weightedSparsity = sparsity * noItems.toDouble
    val bc = BreadCrumbCategory(storeId, storeName, noItems, prob.toArray, weightedSparsity)
    logger.debug("completed categorize")
    bc
  }

}

case class BreadCrumbCategory(storeId: Long, storeName: String, noItems: Long, weightedProb: Array[Double], tokenSparsity: Double) {

  @transient lazy val logger = Logger.getLogger(this.getClass.getName)

  def +(other: BreadCrumbCategory) = {
    require(storeId == other.storeId)
    val probSum: DenseVector[Double] = DenseVector(other.weightedProb: _*) + DenseVector(weightedProb: _*)
    BreadCrumbCategory(storeId, storeName, noItems + other.noItems, probSum.toArray, tokenSparsity + tokenSparsity)
  }

  def category(topLevelModel: TopLevelModel) = {
    logger.debug("start category prediction")
    val (categoryId, category, probability) = topLevelModel.predictCategory(DenseVector(weightedProb: _*))
    val cp = CategoryPrediction(storeId, storeName, categoryId, category, probability, noItems, tokenSparsity)
    logger.debug("completed category prediction")
    cp
  }

  def normalize = (DenseVector(weightedProb) / noItems.toDouble).toArray
}

case class CategoryPrediction(storeId: Long, storeName: String, categoryId: Long, category: String, weightedProb: Double, noItems: Long, tokenSparsity: Double) {
  def prob = weightedProb / noItems
}


object BreadCrumbAnalyzer {


  def main(args: Array[String]) {
    @transient lazy val logger = Logger.getLogger(this.getClass.getName)
    val inputFile = args(0)
    val storeWiseCategoryProbabilities = args(1)
    val storeWiseCategoryPredictions = args(2)
    val breadCrumbPredictions: String = args(3)
    val loggingLevel = if (args.length < 5) Level.WARN else if (args(4).equals("DEBUG")) Level.DEBUG else Level.INFO
    logger.setLevel(loggingLevel)
    val breadCrumbsFile = inputFile
    logger.info(s"The inputfile is $inputFile")
    logger.info(s"The output files are $breadCrumbPredictions and $storeWiseCategoryProbabilities and $storeWiseCategoryPredictions")
    val spark = SparkSession.builder().appName("BreadCrumbAnalyzer").getOrCreate()
    import spark.implicits._
    val breadCrumbsDs = spark.read.json(breadCrumbsFile).as[BreadCrumb].filter(x => x.breadCrumbs.nonEmpty).cache()
    logger.debug("Completed reading breadcrumb json data")
    implicit lazy val model = TopLevelModel()

    /*    Compute the weighted average of store wise probabilities weighted by the breadcrumb frequency*/
    val fields = Seq(
      StructField("storeId", LongType, nullable = false),
      StructField("storeName", StringType, nullable = false),
      StructField("noItems", LongType, nullable = false)
    ) ++ (0 until model.categoryIds.size).map(x => StructField("p_" + model.categoryIds(x), DoubleType, nullable = false))
    val schema = StructType(fields)
    val breadCrumbCategory = breadCrumbsDs.rdd.mapPartitions(iterator => {
      val model = TopLevelModel()
      iterator.map(r => {
        val categoryProbabilities = r.categorize(model)
        val categoryPrediction = categoryProbabilities.category(model)
        (categoryProbabilities, categoryPrediction, r.breadCrumbs)
      }
      )
    }).filter(x => x._1.tokenSparsity > 0.0).cache()

   val predictFields = Seq(
     StructField("storeId",LongType,nullable = false),
     StructField("breadCrumb",StringType,nullable = true),
     StructField("categoryId",LongType,nullable = false),
     StructField( "category",StringType,nullable = false),
     StructField("noItems",LongType,nullable = false),
     StructField("probability",DoubleType,nullable = false),
     StructField("sparsity",DoubleType,nullable = false)
   )
    val predictionsSchema = StructType(predictFields)
    val predictions = breadCrumbCategory.map(x => {
      val breadCrumb = x._3
      val categoryPrediction = x._2
      Row.fromSeq(Seq(categoryPrediction.storeId, breadCrumb, categoryPrediction.categoryId, categoryPrediction.category, categoryPrediction.noItems, categoryPrediction.weightedProb, categoryPrediction.tokenSparsity))
    })
    val predictionsDF = spark.createDataFrame(predictions,predictionsSchema)
    predictionsDF.write.option("compression", "gzip").mode("overwrite").json(breadCrumbPredictions)
    logger.info("completed writing breadcrumb predictions")

    val byStore: RDD[(Long, BreadCrumbCategory)] = breadCrumbCategory.map(x => x._1).keyBy(x => x.storeId)
    val categoryProbabilities: RDD[BreadCrumbCategory] = byStore.reduceByKey(_ + _).values
    val storeWiseProbs = categoryProbabilities.map(r => Row.fromSeq(Seq(r.storeId, r.storeName, r.noItems) ++ r.normalize))
    val storeWiseProbsDF = spark.createDataFrame(storeWiseProbs, schema)
    storeWiseProbsDF.write.option("compression", "gzip").mode("overwrite").json(storeWiseCategoryProbabilities)
    logger.info("completed writing store wise probabilities")

    /*    Compute the store wise weighted aggregate of category predicted for each breadcrumb, weighted by breadcrumb frequency*/
    val categoryForBreadCrumb: Dataset[CategoryPrediction] = breadCrumbCategory.map(x => x._2).toDS()

    val categoryDF = categoryForBreadCrumb.groupBy("storeId", "storeName", "categoryId", "category")
      .agg(expr("sum(weightedProb)/sum(noItems) as probability").as[Double],
        expr("sum(tokenSparsity)/sum(noItems) as sparsity").as[Double]).filter("probability > 0.5").orderBy("storeId")
    categoryDF.write.option("compression", "gzip").mode("overwrite").json(storeWiseCategoryPredictions)
    logger.info("completed writing store wise category predictions")
    spark.stop()
  }

}
