package com.scalafi.dynamics.attribute

import com.scalafi.openbook.Side
import com.scalafi.openbook.orderbook.OrderBook
import framian.Value
import org.scalatest.FlatSpec

class BasicSetSpec extends FlatSpec {
  val order1 = orderMsg(0, 0, 10000, 10, Side.Buy)
  val order2 = orderMsg(100, 0, 10000, 15, Side.Buy)
  val order3 = orderMsg(200, 0, 11000, 20, Side.Sell)

  val orders = Vector(order1, order2, order3)

  val orderBooks = orders.scanLeft(OrderBook(Symbol)) {
    (orderBook, order) => orderBook.update(order)
  }

  val basisSet = BasicSet(BasicSet.Config.default)

  "BasicSet features" should "build valid bid price attribute" in {
    val bid1 = basisSet.bidPrice(1)
    val bid1Volume = basisSet.bidVolume(1)

    val bids1 = orderBooks.map(orderBook => (bid1(orderBook), bid1Volume(orderBook)))
    assert(bids1.last ==(Value(10000), Value(15)))
  }

  it should "prevent from creating wrong metric" in {
    intercept[AssertionError] {
      basisSet.bidPrice(100)
    }
  }

}
