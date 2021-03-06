package com.indix.ml.models

import breeze.linalg.{DenseVector, SparseVector, argmax, convert, normalize, sum}
import breeze.numerics.exp
import com.indix.ml.preprocessing.tokenizers.WordNetTokenizer
import org.apache.log4j.Logger
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.BufferedSource

/**
  * Created by vumaasha on 29/11/16.
  */
class TopLevelModel(modelPath: String) extends Serializable {

  val logger = Logger.getLogger(this.getClass)

  def readJsonFromResource(path: String) = {
    val jsonResource = getClass.getResourceAsStream("/" + path)
    val source: BufferedSource = scala.io.Source.fromInputStream(jsonResource)
    val json = try source.mkString finally source.close()
    json
  }

  val modelJsValue: JValue = {
    val modelJson = readJsonFromResource(modelPath)
    parse(modelJson)
  }

  val categoryMapping: Map[Int, String] = {
    val tuples = for {
      JObject(child) <- modelJsValue
      JField("class_names", JObject(classNames)) <- child
      JField((a: String, JString(b: String))) <- classNames
    } yield (a.toInt, b)
    Map(tuples: _*)
  }

  val categoryIds: Map[Int, Int] = {
    val tuples = for {
      JObject(child) <- modelJsValue
      JField("classes", JObject(classNames)) <- child
      JField((a: String, JInt(b))) <- classNames
    } yield (a.toInt, b.toInt)
    Map(tuples: _*)
  }

  def getCategory(id: Int) = categoryMapping(categoryIds(id))

  val intercept: DenseVector[Double] = {
    val intercepts = for {
      JObject(child) <- modelJsValue
      JField("intercept", JArray(intList)) <- child
      JDouble(b) <- intList
    } yield b
    DenseVector(intercepts: _*)
  }

  val classSparsity: List[Int] = {
    for {
      JObject(child) <- modelJsValue
      JField("coefficients", JObject(coeff)) <- child
      JField(clsId, JObject(clsCoeff)) <- coeff
      JField("size", JInt(size)) <- clsCoeff
    } yield size.toInt
  }

  val vocabulary = {
    val vocabTokens: Seq[(String, Int)] = for {
      JObject(child) <- modelJsValue
      JField("vocabulary", JObject(features)) <- child
      JField(word, JInt(index)) <- features
    } yield (word, index.toInt)
    Map(vocabTokens: _*)
  }


  val coefficients: Seq[SparseVector[Double]] = {
    for {
      JObject(child) <- modelJsValue
      JField("coefficients", JObject(coeff)) <- child
      JField(clsId, JObject(clsInfo)) <- coeff
      JField("class_coefficients", JObject(clsCoeff)) <- clsInfo
    } yield {
      val tuples = for {
        JField(index, JDouble(featureCoeff)) <- clsCoeff
      } yield (index.toInt, featureCoeff)
      SparseVector(vocabulary.size)(tuples: _*)
    }
  }

  def predictProba(X: SparseVector[Double]): DenseVector[Double] = {
    /*    Probability estimation for OvR logistic regression.

            Positive class probabilities are computed as
            1. / (1. + np.exp(-self.decision_function(X)));
            multiclass is handled by normalizing that over all classes */

    val dots = for {
      clsCoeff <- coefficients
    } yield clsCoeff dot X
    val coeffDot = DenseVector(dots: _*) + intercept
    val denom = exp(-1.0 * coeffDot) + 1.0
    val classWiseProb = 1.0 / denom
    val prob = classWiseProb / sum(classWiseProb)
    prob
  }

  val tokenizer = WordNetTokenizer()

  def tokenize(doc: String) = {
    val stems:Array[String] = try {
      tokenizer.tokenize(doc)
    } catch {
      case e:Throwable =>
        logger.warn(s"Problem with tokenizing $doc")
        logger.warn(e)
        Array.empty
    }
    val coocc = for {
      stemI <- stems.zipWithIndex
      stemJ <- stems.zipWithIndex if stemJ._2 > stemI._2
    } yield {
      val (t1: String, t2: String) = if (stemI._1 < stemJ._1) (stemI._1, stemJ._1) else (stemJ._1, stemI._1)
      s"${t1}_$t2"
    }
    for {
      token <- stems ++ coocc if vocabulary contains token
    } yield token
  }

  def vectorize(doc: String) = {
    val tokenFreq = for {
      (token, values) <- tokenize(doc).groupBy(identity)
    } yield (vocabulary(token), values.length.toDouble)
    if (tokenFreq.nonEmpty) {
      val vector = SparseVector(vocabulary.size)(tokenFreq.toSeq: _*)
      normalize(vector / sum(vector), 2.0)
    } else SparseVector(vocabulary.size)(0 -> 0.0)
  }

  def categorize(doc: String) = {
    val (sparsity, prob): (Double, DenseVector[Double]) = predictProba(doc)
    val (categoryId, category, probability) = predictCategory(prob)
    (categoryId, category, probability, sparsity)
  }

  def predictCategory(prob: DenseVector[Double]): (Int, String, Double) = {
    val maxarg = argmax(prob)
    val categoryId = categoryIds(maxarg)
    val category = categoryMapping(categoryId)
    (categoryId, category, prob(maxarg))
  }

  def predictProba(doc: String): (Double, DenseVector[Double]) = {
    val vector: SparseVector[Double] = vectorize(doc)
    val numTokens = vector.activeSize
    val tokenRatio = numTokens.toDouble / vector.size
    val prob = predictProba(vector)
    (tokenRatio, prob)
  }
}

object TopLevelModel {

  def apply(modelPath: String): TopLevelModel = new TopLevelModel(modelPath)

  def apply(): TopLevelModel = new TopLevelModel("L2TopLevelModel.json")
}


