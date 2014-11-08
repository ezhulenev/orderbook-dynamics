package com.scalafi.dynamics.attribute

import com.scalafi.openbook.Side
import com.scalafi.openbook.orderbook.OrderBook
import framian.Value
import org.scalatest.FlatSpec

class TimeInsensitiveSetSpec extends FlatSpec {

  val order1 = orderMsg(0, 0, 10000, 10, Side.Buy)
  val order2 = orderMsg(100, 0, 10000, 15, Side.Buy)
  val order3 = orderMsg(200, 0, 11000, 20, Side.Sell)
  val order4 = orderMsg(200, 0, 12000, 30, Side.Sell)
  val order5 = orderMsg(200, 0, 14000, 40, Side.Sell)

  val orders = Vector(order1, order2, order3, order4, order5)

  val orderBooks = orders.scanLeft(OrderBook(Symbol)) {
    (orderBook, order) => orderBook.update(order)
  }

  val timeInsensitiveSet = TimeInsensitiveSet(TimeInsensitiveSet.Config.default)

  "TimeInsensitiveSet" should "build valid price spreads stream" in {
    val priceSpread1 = timeInsensitiveSet.priceSpread(1)
    val priceSpreads1 = orderBooks.map(priceSpread1(_))

    assert(priceSpreads1.last == Value(1000))
  }

  it should "build valid volume spreads stream" in {
    val volumeSpread1 = timeInsensitiveSet.volumeSpread(1)
    val volumeSpreads1 = orderBooks.map(volumeSpread1(_))

    assert(volumeSpreads1.last == Value(5))
  }

  it should "build valid mid price stream" in {
    val midPrice1 = timeInsensitiveSet.midPrice(1)
    val midPrices1 = orderBooks.map(midPrice1(_))

    assert(midPrices1.last == Value(10500))
  }

  it should "build valid ask step stream" in {
    val askStep1 = timeInsensitiveSet.askStep(1)
    val askSteps1 = orderBooks.map(askStep1(_))

    assert(askSteps1.last == Value(1000))
  }

  it should "build valid mean ask price stream" in {
    val meanAsk = timeInsensitiveSet.meanAsk
    val meanAsks = orderBooks.map(meanAsk(_))

    val expectedMean = (order3.priceNumerator.toDouble + order4.priceNumerator.toDouble + order5.priceNumerator.toDouble) / 3
    assert(meanAsks.last == Value(expectedMean))
  }

  it should "build valid mean bid price stream" in {
    val meanBid = timeInsensitiveSet.meanBid
    val meanBids = orderBooks.map(meanBid(_))

    val expectedMean = (order1.priceNumerator.toDouble + order2.priceNumerator.toDouble) / 2
    assert(meanBids.last == Value(expectedMean))
  }

  it should "build valid price & volume accumulators" in {
    val accumulatedPrice = timeInsensitiveSet.accumulatedPriceSpread
    val accumulatedVolume = timeInsensitiveSet.accumulatedVolumeSpread

    val acc = orderBooks.map(ob => (accumulatedPrice(ob), accumulatedVolume(ob)))

    val expectedAcc = (Value(1000), Value(5))
    assert(acc.last == expectedAcc)
  }
}