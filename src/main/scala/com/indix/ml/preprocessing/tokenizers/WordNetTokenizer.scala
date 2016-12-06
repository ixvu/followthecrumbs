package com.indix.ml.preprocessing.tokenizers

import java.io.File

import edu.mit.jwi.RAMDictionary
import edu.mit.jwi.data.ILoadPolicy
import edu.mit.jwi.morph.WordnetStemmer
import scala.collection.JavaConverters._

/**
  * Created by vumaasha on 5/12/16.
  */
class WordNetTokenizer(wordNetDir:String) extends Serializable{
  val dict = new RAMDictionary(new File(wordNetDir))
  dict.setLoadPolicy(ILoadPolicy.IMMEDIATE_LOAD)
  dict.open()
  val stemmer = new WordnetStemmer(dict)

  def tokenize(doc: String) = {
    val token_pattern = "\\b\\w\\w+\\b".r
    val stems = {
      for {
        token <- token_pattern.findAllIn(doc.toLowerCase)
        stem <- stemmer.findStems(token, null).asScala
      } yield stem
    }.toArray
    stems
  }
}

object WordNetTokenizer {
  def apply(): WordNetTokenizer = new WordNetTokenizer("/opt/WordNet-3.0/dict")
}
