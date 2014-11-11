package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook
import framian.Cell

object BasicAttribute {
  def from[T](f: OrderBook => Cell[T]) = new BasicAttribute[T] {
    def apply(orderBook: OrderBook): Cell[T] = f(orderBook)
  }
}

sealed trait BasicAttribute[T] extends Serializable { self =>
  def apply(orderBook: OrderBook): Cell[T]

  def map[T2](f: T => T2): BasicAttribute[T2] = new BasicAttribute[T2] {
    def apply(orderBook: OrderBook): Cell[T2] = self(orderBook).map(f)
  }
}

object BasicSet {

  def apply(config: BasicSet.Config): BasicSet =
    new BasicSet(config)

  trait Config extends Serializable {
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

class BasicSet private[attribute] (val config: BasicSet.Config) extends Serializable {
  private[attribute] def askPrice(orderBook: OrderBook)(i: Int): Cell[Int] = {
    Cell.fromOption {
      orderBook.sell.keySet.drop(i - 1).headOption
    }
  }

  private[attribute] def askVolume(orderBook: OrderBook)(i: Int) = {
    askPrice(orderBook)(i).map(orderBook.sell)
  }

  private[attribute] def bidPrice(orderBook: OrderBook)(i: Int): Cell[Int] = {
    Cell.fromOption {
      val bidPrices = orderBook.buy.keySet
      if (bidPrices.size >= i) {
        bidPrices.drop(bidPrices.size - i).headOption
      } else None
    }
  }

  private[attribute] def bidVolume(orderBook: OrderBook)(i: Int) = {
    bidPrice(orderBook)(i).map(orderBook.buy)
  }

  private def checkLevel[T](i: Int)(f: =>T): T = {
    config.checkLevel(i)
    f
  }

  private def attribute[T](f: OrderBook => Cell[T]): BasicAttribute[T] = new BasicAttribute[T] {
    def apply(orderBook: OrderBook): Cell[T] = f(orderBook)
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

  val meanPrice: BasicAttribute[Double] = {
    val ask1 = askPrice(1)
    val bid1 = bidPrice(1)
    BasicAttribute.from(orderBook =>
      ask1(orderBook).zipMap(bid1(orderBook)) {
        (ask, bid) => (ask.toDouble + bid.toDouble) / 2
      })
  }
}