package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook

sealed trait BasicAttribute[T] {
  def apply(orderBook: OrderBook): Option[T]
}

object BasicSet {

  def apply(config: BasicSet.Config): BasicSet =
    new BasicSet(config)

  trait Config {
    def orderBookDepth: Int

    def checkLevel(i: Int) = {
      assume(i > 0, s"Level index should be greater then 0")
      assume(i <= orderBookDepth, s"Level index should be less then $orderBookDepth")
    }
  }

  object Config {
    lazy implicit val default = new Config {
      val orderBookDepth = 10
    }
  }
}

class BasicSet private[attribute] (config: BasicSet.Config) {
  private[attribute] def askPrice(orderBook: OrderBook)(i: Int): Option[Int] = {
    orderBook.sell.keySet.drop(i - 1).headOption
  }

  private[attribute] def askVolume(orderBook: OrderBook)(i: Int) = {
    askPrice(orderBook)(i).map(orderBook.sell)
  }

  private[attribute] def bidPrice(orderBook: OrderBook)(i: Int): Option[Int] = {
    val bidPrices = orderBook.buy.keySet
    if (bidPrices.size >= i) {
      bidPrices.drop(bidPrices.size - i).headOption
    } else None
  }

  private[attribute] def bidVolume(orderBook: OrderBook)(i: Int) = {
    bidPrice(orderBook)(i).map(orderBook.buy)
  }

  private def checkLevel[T](i: Int)(f: =>T): T = {
    config.checkLevel(i)
    f
  }

  private def attribute[T](f: OrderBook => Option[T]): BasicAttribute[T] = new BasicAttribute[T] {
    def apply(orderBook: OrderBook): Option[T] = f(orderBook)
  }

  def askPrice(i: Int): BasicAttribute[Int] = checkLevel(i) {
    attribute(askPrice(_)(i))
  }

  def bidPrice(i: Int): BasicAttribute[Int] = checkLevel(i) {
    attribute(bidPrice(_)(i))
  }

  def askVolume(i: Int): BasicAttribute[Int] = checkLevel(i) {
    attribute(askVolume(_)(i))
  }

  def bidVolume(i: Int): BasicAttribute[Int] = checkLevel(i) {
    attribute(bidVolume(_)(i))
  }
}