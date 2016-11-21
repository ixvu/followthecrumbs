package com.indix.ml.analysis.breadcrumbs

import java.io.File

import edu.mit.jwi.RAMDictionary
import edu.mit.jwi.data.ILoadPolicy
import edu.mit.jwi.item.POS
import edu.mit.jwi.morph.WordnetStemmer

import scala.collection.JavaConverters._
import org.apache.log4j.{Level,Logger}

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

  def tokenize(doc: String): Array[String] = {
    try {
      val token_pattern = "\\b\\w\\w+\\b".r
      token_pattern.findAllIn(doc.toLowerCase).flatMap((token) => {
        stemmer.findStems(token, POS.NOUN)
      }.asScala).toSet.toArray
    } catch {
      case e: Throwable  => {
        logger.info(s"The errored document is $doc")
        logger.info(e)
        Array.empty
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val tokens = tokenize("\"Home > Watches > Ice-Watch Watches > Ice-Watch Unisex Watches > Ice-Pure")
    tokenize("")
    tokens.foreach(println(_))
  }
}
