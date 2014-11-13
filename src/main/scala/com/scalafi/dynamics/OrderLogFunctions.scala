package com.scalafi.dynamics

import com.scalafi.dynamics.attribute.{LabelEncode, LabeledPointsExtractor}
import com.scalafi.openbook.OpenBookMsg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.SparkContext._

case class LabeledOrderLog[L: LabelEncode](symbol: String, labeledPoints: RDD[LabeledPoint])

class OrderLogFunctions(orderLog: RDD[OpenBookMsg]) extends Serializable {
  private val log = LoggerFactory.getLogger(classOf[OrderLogFunctions])

  private def sc = orderLog.context

  def symbols(): Set[String] = {
    log.debug(s"Get symbols from order log. Order log size: ${orderLog.count()}")
    orderLog.map(_.symbol).countByValue().keys.toSet
  }

  def countBySymbol(): Map[String, Long] = {
    log.debug(s"Count orders by symbol. Order log size: ${orderLog.count()}")
    orderLog.map(_.symbol).countByValue().toMap
  }

  def extractLabeledData[L: LabelEncode](extractor: String => LabeledPointsExtractor[L]): Vector[LabeledOrderLog[L]] = {
    log.debug(s"Extract labeled data from order log. Order log size: ${orderLog.count()}")
    extractLabeledData(orderLog, extractor)
  }

  def extractLabeledData[L: LabelEncode](symbols: String*)(extractor: String => LabeledPointsExtractor[L]): Vector[LabeledOrderLog[L]] = {
    log.debug(s"Extract labeled data from order log for symbols: [${symbols.mkString(", ")}]. Order log size: ${orderLog.count()}")
    extractLabeledData(orderLog.filter(order => symbols.contains(order.symbol)), extractor)
  }

  private def extractLabeledData[L: LabelEncode](input: RDD[OpenBookMsg], extractor: String => LabeledPointsExtractor[L]): Vector[LabeledOrderLog[L]] = {

    type LabeledData = (String, Vector[LabeledPoint])

    val labeledData: RDD[LabeledData] =
      (input.groupBy(_.symbol) map { case (symbol, orders) =>
        (symbol, extractor(symbol).labeledPoints(orders.toVector.sortBy(order => (order.sourceTime, order.sourceTimeMicroSecs))))
      }).cache()

    val symbols = labeledData.keys.countByValue().keys.toSet

    symbols.toVector map { symbol =>
      val filtered = labeledData.filter(_._1 == symbol)
      assume(filtered.count() == 1, s"Expected single record for symbol: $symbol. Got: ${filtered.count()}")
      val (_, labeledPoints) = filtered.first()
      LabeledOrderLog(symbol, sc.parallelize(labeledPoints))
    }
  }
}
