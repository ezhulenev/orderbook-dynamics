package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook
import org.scalatest.FlatSpec

import scala.collection.immutable.TreeMap

class LabelSpec extends FlatSpec {

  // Mean price = (100 + 110) / 2 = 105
  val lowerMeanPriceOrderBook = new OrderBook("AAPL", TreeMap(100 -> 1), TreeMap(110 ->1))

  // Mean price = (110 + 120) / 2 = 115
  val higherMeanPriceOrderBook = new OrderBook("AAPL", TreeMap(110 -> 1), TreeMap(120 -> 1))

  "MeanPriceMovementLabel" should "correctly assign label" in {
    assert(MeanPriceMovementLabel(lowerMeanPriceOrderBook, higherMeanPriceOrderBook) == Some(MeanPriceMove.Up))
    assert(MeanPriceMovementLabel(higherMeanPriceOrderBook, lowerMeanPriceOrderBook) == Some(MeanPriceMove.Down))
    assert(MeanPriceMovementLabel(lowerMeanPriceOrderBook, lowerMeanPriceOrderBook) == Some(MeanPriceMove.Stationary))
  }
}
