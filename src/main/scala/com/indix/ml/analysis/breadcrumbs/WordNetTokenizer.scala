package com.indix.ml.analysis.breadcrumbs

import java.io.File

import breeze.linalg.{SparseVector, sum, normalize}
import edu.mit.jwi.RAMDictionary
import edu.mit.jwi.data.ILoadPolicy
import edu.mit.jwi.morph.WordnetStemmer
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConverters._
import scala.io.BufferedSource

/**
  * Created by vumaasha on 18/11/16.
  */
object WordNetTokenizer {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.setLevel(Level.INFO)
  val wordNetDir = "/opt/WordNet-3.0/dict"
  val dict = new RAMDictionary(new File(wordNetDir))
  dict.setLoadPolicy(ILoadPolicy.IMMEDIATE_LOAD)
  dict.open()
  val stemmer = new WordnetStemmer(dict)

  val vocabResource = getClass.getResourceAsStream("/TopLevelVocabulary.json")
  val source: BufferedSource = scala.io.Source.fromInputStream(vocabResource)
  val vocabularyJson = try source.mkString finally source.close()
  val jsVal = parse(vocabularyJson)
  val vocabulary:Map[String,Int] = {
    val vocabTokens:Seq[(String,Int)] = for {
      JObject(child) <- jsVal
      JField(a,JInt(b)) <- child
    } yield (a,b.toInt)
    Map(vocabTokens:_*)
  }

  val vocabSize = vocabulary.size

  def tokenize(doc: String) = {
    val token_pattern = "\\b\\w\\w+\\b".r
    val stems = {
      for {
        token <- token_pattern.findAllIn(doc.toLowerCase)
        stem <- stemmer.findStems(token, null).asScala
      } yield stem
    }.toArray
    val coocc = for {
      stemI <- stems.zipWithIndex
      stemJ <- stems.zipWithIndex if stemJ._2 > stemI._2
    } yield {
      val (t1:String,t2:String) = if (stemI._1 < stemJ._1) (stemI._1,stemJ._1) else (stemJ._1,stemI._1)
      s"${t1}_$t2"
    }
    for {
      token <- stems ++ coocc if vocabulary contains token
    } yield token
  }

  def vectorize(doc:String) = {
      val tokenFreq = for {
        (token,values) <- tokenize(doc).groupBy(identity)
      } yield (vocabulary(token),values.size.toDouble)
    val vector = SparseVector(vocabSize)(tokenFreq.toSeq:_*)
    normalize(vector / sum(vector),2.0)
  }

  def main(args: Array[String]): Unit = {
    val tokens = vectorize("electronics electronics watches battery cable microphone supply power player")
    tokens.foreach(println(_))
  }
}
