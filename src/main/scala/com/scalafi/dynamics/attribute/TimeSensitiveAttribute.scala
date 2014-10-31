package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook
import com.scalafi.openbook.{OpenBookMsg, Side}
import framian.{Value, NA, Cell}

import scala.concurrent.duration.FiniteDuration

trait TimeSensitiveAttribute[T] {
  def apply(ordersTrail: Vector[OpenBookMsg]): Cell[T]
}

object TimeSensitiveSet {
  
  def apply(config: TimeSensitiveSet.Config): TimeSensitiveSet =
    new TimeSensitiveSet(config)

  trait Config {
    def duration: FiniteDuration
    def basicSetConfig: BasicSet.Config
  }

  object Config {
    import scala.concurrent.duration._

    def apply(d: FiniteDuration, basicConfig: BasicSet.Config = BasicSet.Config.default) =
      new Config {
        val duration = d
        val basicSetConfig = basicConfig
      }

    implicit val default = apply(1.second)
  }
}

class TimeSensitiveSet private[attribute](config: TimeSensitiveSet.Config) {

  def trail(current: Vector[OpenBookMsg], order: OpenBookMsg): Vector[OpenBookMsg] = {
    val cutOffTime = order.sourceTime - config.duration.toMillis
    current.dropWhile(_.sourceTime < cutOffTime) :+ order
  }

  private[attribute] type Extractor[T] = OrderBook => Int => Cell[T]

  private[attribute] def spread(orderBook: OrderBook)(i: Int, l: Extractor[Int], r: Extractor[Int]): Cell[Int] = {
    (l(orderBook)(i) zipMap r(orderBook)(i))((lv, rv) => lv - rv)
  }

  private[attribute] def arrivalRate(f: OpenBookMsg => Boolean)(ordersTrail: Vector[OpenBookMsg]): Cell[Double] =
      (Cell.fromOption(ordersTrail.headOption) zipMap Cell.fromOption(ordersTrail.lastOption)) {
        case (head, last) =>
          val diff = last.sourceTime - head.sourceTime
          if (diff == 0) NA else Value(ordersTrail.count(f).toDouble / diff)
      }.flatten

  private def attribute[T](f: Vector[OpenBookMsg] => Cell[T]): TimeSensitiveAttribute[T] = new TimeSensitiveAttribute[T] {
    def apply(ordersTrail: Vector[OpenBookMsg]): Cell[T] = f(ordersTrail)
  }

  def bidArrivalRate: TimeSensitiveAttribute[Double] =
    attribute(arrivalRate(_.side == Side.Buy)(_))

  def askArrivalRate: TimeSensitiveAttribute[Double] =
    attribute(arrivalRate(_.side == Side.Sell)(_))
}