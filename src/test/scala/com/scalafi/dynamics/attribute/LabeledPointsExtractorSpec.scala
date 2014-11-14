package com.scalafi.dynamics.attribute

import com.scalafi.openbook.Side
import org.scalatest.FlatSpec


class LabeledPointsExtractorSpec extends FlatSpec {

  val order1 = orderMsg(1, 0, 10000, 10, Side.Buy)       // mean: N/A
  val order2 = orderMsg(102, 0, 20000, 15, Side.Sell)    // mean: 15000
  val order3 = orderMsg(203, 0, 11000, 20, Side.Buy)     // mean: 15500
  val order4 = orderMsg(1014, 0, 18000, 20, Side.Sell)   // mean: 14500
  val order5 = orderMsg(1185, 0, 12000, 60, Side.Buy)    // mean: 15000
  val order6 = orderMsg(1196, 0, 16000, 24, Side.Sell)   // mean: 14000
  val order7 = orderMsg(1227, 0, 13000, 50, Side.Buy)    // mean: 14500
  val order8 = orderMsg(2228, 0, 14000, 50, Side.Sell)   // mean: 13500

  val extractor = {
    import LabeledPointsExtractor._
    (LabeledPointsExtractor.newBuilder()
      += basic(_.meanPrice)
    ).result("AAPL", MeanPriceMovementLabel)
  }

  "LabeledPointsExtractor" should "extract valid cursors from orders log" in {
    val orders = Vector(order1, order2, order3, order4)
    val traversal = new extractor.OrdersCursorTraversal(orders)

    import extractor.{AttributesCursor, LabelCursor}

    val attributes = traversal.attributes.toVector
    assert(attributes.size == 4)
    assert(attributes(0) == AttributesCursor(order1, orderBook(order1), Vector(order1)))
    assert(attributes(1) == AttributesCursor(order2, orderBook(order1, order2), Vector(order1, order2)))
    assert(attributes(2) == AttributesCursor(order3, orderBook(order1, order2, order3), Vector(order1, order2, order3)))
    // First order should be removed from trail
    assert(attributes(3) == AttributesCursor(order4, orderBook(order1, order2, order3, order4), Vector(order2, order3, order4)))

    val labels = traversal.labels.toVector
    assert(labels.size == 4)
    assert(labels(0) == LabelCursor(order1, orderBook(order1)))
    assert(labels(1) == LabelCursor(order2, orderBook(order1, order2)))
    assert(labels(2) == LabelCursor(order3, orderBook(order1, order2, order3)))
    assert(labels(3) == LabelCursor(order4, orderBook(order1, order2, order3, order4)))
  }

  it should "build valid cursor pairs" in {
    val orders = Vector(order1, order2, order3, order4, order5, order6, order7, order8)
    val traversal = new extractor.OrdersCursorTraversal(orders)

    import extractor.{AttributesCursor, LabelCursor}

    val pairs = traversal.cursor.toVector

    // Expected pairs:
    // order1 -> order4
    // order2 -> order5
    // order3 -> order7
    // order4 -> order8
    // order5 -> order8
    // order6 -> order8
    // order7 -> order8

    assert(pairs(0)._1 == AttributesCursor(order1, orderBook(order1), Vector(order1)))
    assert(pairs(0)._2 == LabelCursor(order4, orderBook(order1, order2, order3, order4)))

    assert(pairs(1)._1 == AttributesCursor(order2, orderBook(order1, order2), Vector(order1, order2)))
    assert(pairs(1)._2 == LabelCursor(order5, orderBook(order1, order2, order3, order4, order5)))

    assert(pairs(2)._1 == AttributesCursor(order3, orderBook(order1, order2, order3), Vector(order1, order2, order3)))
    assert(pairs(2)._2 == LabelCursor(order7, orderBook(order1, order2, order3, order4, order5, order6, order7)))

    assert(pairs(3)._1 == AttributesCursor(order4, orderBook(order1, order2, order3, order4), Vector(order2, order3, order4)))
    assert(pairs(3)._2 == LabelCursor(order8, orderBook(order1, order2, order3, order4, order5, order6, order7, order8)))
  }

  it should "build valid labeled points" in {
    val orders = Vector(order1, order2, order3, order4, order5, order6, order7, order8)
    val labeledPoints = extractor.labeledPoints(orders)

    assert(labeledPoints.size == 6)

    assert(labeledPoints(0).label == 2) // stationary: order2 -> order5
    assert(labeledPoints(0).features.apply(0) == 15000)

    assert(labeledPoints(1).label == 1) // down: order3 -> order7
    assert(labeledPoints(1).features.apply(0) == 15500)

    assert(labeledPoints(2).label == 1) // down: order4 -> order8
    assert(labeledPoints(2).features.apply(0) == 14500)

    assert(labeledPoints(3).label == 1) // down: order5 -> order8
    assert(labeledPoints(3).features.apply(0) == 15000)

    assert(labeledPoints(4).label == 1) // down: order6 -> order8
    assert(labeledPoints(4).features.apply(0) == 14000)

    assert(labeledPoints(5).label == 1) // down: order7 -> order8
    assert(labeledPoints(5).features.apply(0) == 14500)
  }
}
