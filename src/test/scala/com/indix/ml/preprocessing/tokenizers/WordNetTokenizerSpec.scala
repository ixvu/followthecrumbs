package com.indix.ml.preprocessing.tokenizers

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by vumaasha on 2/12/16.
  */
class WordNetTokenizerSpec extends FlatSpec with Matchers {
  "Tokenizer" should "return tokens" in {
    val tokenizer = WordNetTokenizer()
    val tokens = tokenizer.tokenize("home kitchen")
    tokens should contain allOf ("home","kitchen")
  }
}
