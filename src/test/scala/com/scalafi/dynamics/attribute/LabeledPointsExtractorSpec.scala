package com.scalafi.dynamics.attribute

import com.scalafi.dynamics.attribute.LabeledPointsExtractor.AttributeSet
import com.scalafi.openbook.Side
import org.scalatest.FlatSpec
import scala.concurrent.duration._

class LabeledPointsExtractorSpec extends FlatSpec {

  val order1 = orderMsg(0, 0, 10000, 10, Side.Buy)       // mean: N/A
  val order2 = orderMsg(100, 0, 20000, 15, Side.Sell)    // mean: 15000
  val order3 = orderMsg(200, 0, 11000, 20, Side.Buy)     // mean: 15500
  val order4 = orderMsg(1010, 0, 18000, 20, Side.Sell)   // mean: 14500
  val order5 = orderMsg(1180, 0, 12000, 60, Side.Buy)    // mean: 15000
  val order6 = orderMsg(1190, 0, 16000, 24, Side.Sell)   // mean: 14000
  val order7 = orderMsg(1220, 0, 13000, 50, Side.Buy)    // mean: 14500
  val order8 = orderMsg(2220, 0, 14000, 50, Side.Sell)   // mean: 13500

  val basic = {
    val basicSet = BasicSet(BasicSet.Config.default)
    AttributeSet[BasicSet, BasicAttribute[Double]](basicSet, Vector(_.meanPrice))
  }

  val timeInsensitive = {
    val timeInsensitiveSet = TimeInsensitiveSet(TimeInsensitiveSet.Config.default)
    AttributeSet(timeInsensitiveSet, Vector.empty[TimeInsensitiveSet => TimeInsensitiveAttribute[Double]])
  }

  val timeSensitive = {
    val timeSensitiveSet = TimeSensitiveSet(TimeSensitiveSet.Config(1.second))
    AttributeSet(timeSensitiveSet, Vector.empty[TimeSensitiveSet => TimeSensitiveAttribute[Double]])
  }

  val extractor = new LabeledPointsExtractor(Symbol, MeanPriceMovementLabel)(basic)(timeInsensitive)(timeSensitive)


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

    assert(pairs.size == 4)

    // Expected pairs:  order1 -> order4, order2 -> order5, order3 -> order7, order4 -> order8

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

    assert(labeledPoints.size == 3)

    assert(labeledPoints(0).label == 2) // stationary: order2 -> order5
    assert(labeledPoints(0).features.apply(0) == 15000)

    assert(labeledPoints(1).label == 1) // down: order3 -> order7
    assert(labeledPoints(1).features.apply(0) == 15500)

    assert(labeledPoints(2).label == 1) // down: order4 -> order8
    assert(labeledPoints(2).features.apply(0) == 14500)
  }
}
