package com.indix.ml

import breeze.linalg.{DenseVector, SparseVector}
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.BufferedSource

/**
  * Created by vumaasha on 29/11/16.
  */
class TopLevelModel(modelPath: String) {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.setLevel(Level.INFO)

  def readJsonFromResource(path: String) = {
    val jsonResource = getClass.getResourceAsStream("/" + path)
    val source: BufferedSource = scala.io.Source.fromInputStream(jsonResource)
    val json = try source.mkString finally source.close()
    json
  }

  val modelJsValue:JValue = {
    val modelJson = readJsonFromResource(modelPath)
    parse(modelJson)
  }

  val categoryMapping: Map[Int,String] = {
    val tuples = for {
      JObject(child) <- modelJsValue
      JField("class_names", JObject(classNames)) <- child
      JField((a:String,JString(b:String))) <- classNames
    } yield (a.toInt, b)
    Map(tuples: _*)
  }

  val categoryIds: Map[Int,Int] = {
    val tuples = for {
      JObject(child) <- modelJsValue
      JField("classes", JObject(classNames)) <- child
      JField((a:String,JInt(b))) <- classNames
    } yield (a.toInt, b.toInt)
    Map(tuples: _*)
  }

  def getCategory(id:Int) = categoryMapping(categoryIds(id))

  val intercept:DenseVector[Double] = {
     val intercepts = for {
       JObject(child) <- modelJsValue
       JField("intercept",JArray(intList)) <- child
       JDouble(b) <- intList
     } yield b
    DenseVector(intercepts:_*)
  }
}

object TopLevelModel {
  def apply(modelPath: String): TopLevelModel = new TopLevelModel(modelPath)

  def apply(): TopLevelModel = new TopLevelModel("TopLevelModel.json")
}




