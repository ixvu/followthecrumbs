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

    val doc = "Books > Performing Arts > Television > History & Criticism > Everything I Ever Needed to Know about _____* I Learned from Monty Python: History, Art, Poetry, Communism, Philosophy, the Media, Birth, Death, Religion, Literature, Latin, Transvestites, Botany, the French, Class Systems, Mythology, Fish Slapping"
    val docTokens = tokenizer.tokenize(doc)
    docTokens.length should be > 0

    val hypendoc = "show-case"
    val hypendocTokens = tokenizer.tokenize(hypendoc)
    hypendocTokens should contain allOf ("show","case")
  }
}
