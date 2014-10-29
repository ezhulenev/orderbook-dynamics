package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook

sealed trait TimeInsensitiveAttribute[T] {
  def apply(orderBook: OrderBook): Option[T]
}

object TimeInsensitiveSet {

  def apply(config: TimeInsensitiveSet.Config): TimeInsensitiveSet =
    new TimeInsensitiveSet(config)

  trait Config {
    def basicSetConfig: BasicSet.Config
  }

  object Config {
    lazy implicit val default = new Config {
      val basicSetConfig = BasicSet.Config.default
    }
  }
}

class TimeInsensitiveSet private[attribute](config: TimeInsensitiveSet.Config) {

  import scalaz.std.option._
  import scalaz.syntax.applicative._
  
  private val basicSet = BasicSet(config.basicSetConfig)
  
  private[attribute] type Extractor[T] = OrderBook => Int => Option[T]

  private[attribute] def spread(orderBook: OrderBook)(i: Int, l: Extractor[Int], r: Extractor[Int]): Option[Int] = {
    ^(l(orderBook)(i), r(orderBook)(i))((lv, rv) => lv - rv)
  }

  private[attribute] def mean(orderBook: OrderBook)(f: Extractor[Int]): Option[Double] = {
    val definedValues =
      (1 to config.basicSetConfig.orderBookDepth) map (i => f(orderBook)(i)) takeWhile(_.isDefined) map (_.get)
    
    if (definedValues.nonEmpty) {
      val sum = definedValues.sum.toDouble
      val n = definedValues.size

      Some(sum / n)
    } else None
  }

  private[attribute] def acc(orderBook: OrderBook)(ask: Extractor[Int], bid: Extractor[Int]): Option[Int] = {
    val spreads =
      (1 to config.basicSetConfig.orderBookDepth) map(i => ^(ask(orderBook)(i), bid(orderBook)(i))((a, b) => a - b)) takeWhile(_.isDefined) map(_.get)

    if (spreads.isEmpty) None else Some(spreads.sum)
  }

  private def checkLevel[T](i: Int)(f: =>T): T = {
    config.basicSetConfig.checkLevel(i)
    f
  }

  private def attribute[T](f: OrderBook => Option[T]): TimeInsensitiveAttribute[T] = new TimeInsensitiveAttribute[T] {
    def apply(orderBook: OrderBook): Option[T] = f(orderBook)
  }

  def priceSpread(i: Int): TimeInsensitiveAttribute[Int] = checkLevel(i) {
    import basicSet.{askPrice, bidPrice}
    attribute(spread(_)(i, askPrice, bidPrice))
  }

  def volumeSpread(i: Int): TimeInsensitiveAttribute[Int] = checkLevel(i) {
    import basicSet.{askVolume, bidVolume}
    attribute(spread(_)(i, askVolume, bidVolume))
  }

  def midPrice(i: Int): TimeInsensitiveAttribute[Int] = checkLevel(i) {
    import basicSet.{askPrice, bidPrice}
    attribute(ob => ^(askPrice(ob)(i), bidPrice(ob)(i))((ask, bid) => (ask + bid) / 2))
  }

  def bidStep(i: Int): TimeInsensitiveAttribute[Int] = checkLevel(i) {
    import basicSet.bidPrice
    attribute(ob => ^(bidPrice(ob)(i), bidPrice(ob)(i+1))((bid1, bid2) => math.abs(bid1 - bid2)))
  }

  def askStep(i: Int): TimeInsensitiveAttribute[Int] = checkLevel(i) {
    import basicSet.askPrice
    attribute(ob => ^(askPrice(ob)(i), askPrice(ob)(i+1))((ask1, ask2) => math.abs(ask1 - ask2)))
  }

  def meanAsk: TimeInsensitiveAttribute[Double] = {
    import basicSet.askPrice
    attribute(mean(_)(askPrice))
  }

  def meanBid: TimeInsensitiveAttribute[Double] = {
    import basicSet.bidPrice
    attribute(mean(_)(bidPrice))
  }

  def meanAskVolume: TimeInsensitiveAttribute[Double] = {
    import basicSet.askVolume
    attribute(mean(_)(askVolume))
  }

  def meanBidVolume: TimeInsensitiveAttribute[Double] = {
    import basicSet.bidVolume
    attribute(mean(_)(bidVolume))
  }

  def accumulatedPriceSpread: TimeInsensitiveAttribute[Int] = {
    import basicSet.{askPrice, bidPrice}
    attribute(acc(_)(askPrice, bidPrice))
  }

  def accumulatedVolumeSpread: TimeInsensitiveAttribute[Int] = {
    import basicSet.{askVolume, bidVolume}
    attribute(acc(_)(askVolume, bidVolume))
  }
}