package com.scalafi.dynamics.attribute

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

object LabeledPointsExtractorBuilder {
  def newBuilder: LabeledPointsExtractorBuilder = ???
}

trait LabeledPointsExtractorBuilder {
  def add[T](attribute: BasicAttribute[T]): this.type
  def add[T](attribute: TimeInsensitiveAttribute[T]): this.type
  def add[T](attribute: TimeSensitiveAttribute[T]): this.type

  def +=[T](attribute: BasicAttribute[T]): this.type = add(attribute)
  def +=[T](attribute: TimeInsensitiveAttribute[T]): this.type = add(attribute)
  def +=[T](attribute: TimeSensitiveAttribute[T]): this.type = add(attribute)
  
  def result(): LabeledPointsExtractor
}

trait LabeledPointsExtractor {
  
  LabeledPoint
  

}
