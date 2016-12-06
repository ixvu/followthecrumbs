package com.indix.ml.analysis.breadcrumbs

import breeze.linalg.DenseVector
import com.indix.ml.models.TopLevelModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

case class BreadCrumb(storeId: Long, storeName: String, breadCrumbs: String, noItems: Long) {
  def categorize(topLevelModel: TopLevelModel) = {
    val prob = topLevelModel.predictProba(breadCrumbs) * noItems.toDouble
    BreadCrumbCategory(storeId, storeName, noItems, prob.toArray)
  }
}

case class BreadCrumbCategory(storeId: Long, storeName: String, noItems: Long, prob: Array[Double]) {

  def +(other: BreadCrumbCategory) = {
    require(storeId == other.storeId)
    val probSum: DenseVector[Double] = DenseVector(other.prob:_*) + DenseVector(prob:_*)
    BreadCrumbCategory(storeId, storeName, noItems + other.noItems, probSum.toArray)
  }

  def normalize = (DenseVector(prob) / noItems.toDouble).toArray
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
    val fields = Seq(
      StructField("storeId", LongType, nullable = false),
      StructField("storeName", StringType, nullable = false),
      StructField("noItems", LongType, nullable = false)
    ) ++ (0 to model.categoryIds.size - 1).map(x => StructField("p_" + model.categoryIds(x), DoubleType, nullable = false))
    val schema = StructType(fields)

    val breadCrumbCategory = breadCrumbsDs.rdd.map(r => {
      val model = TopLevelModel()
      r.categorize(model)
    })
    val byStore: RDD[(Long, BreadCrumbCategory)] = breadCrumbCategory.keyBy(x => x.storeId)
    val storeWiseProbs = byStore.reduceByKey(_ + _).values.map(r => Row.fromSeq(Seq(r.storeId, r.storeName, r.noItems) ++ r.normalize))

    val storeWiseProbsDF = spark.createDataFrame(storeWiseProbs, schema)
    storeWiseProbsDF.show()
    storeWiseProbsDF.coalesce(1).write.mode("overwrite").json(outputFile)
    spark.stop()
  }

}
