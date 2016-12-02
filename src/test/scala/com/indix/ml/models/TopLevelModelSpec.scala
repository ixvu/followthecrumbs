package com.indix.ml.models

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by vumaasha on 2/12/16.
  */
class TopLevelModelSpec extends FlatSpec with Matchers{

  behavior of "TopLevelModelSpec"

  it should "predict home & kitchen category" in {
    val model = TopLevelModel()
    val prediction = model.predictCategory("home kitchen")
    prediction._1 should be (10164)

  }

}
