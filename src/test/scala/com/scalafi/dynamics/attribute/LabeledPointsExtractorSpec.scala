package com.scalafi.dynamics.attribute

import com.scalafi.dynamics.attribute.LabeledPointsExtractor.AttributeSet
import com.scalafi.openbook.Side
import org.scalatest.FlatSpec
import scala.concurrent.duration._

class LabeledPointsExtractorSpec extends FlatSpec {

  val order1 = orderMsg(0, 0, 10000, 10, Side.Buy)
  val order2 = orderMsg(100, 0, 10000, 15, Side.Buy)
  val order3 = orderMsg(200, 0, 11000, 20, Side.Sell)
  val order4 = orderMsg(1010, 0, 12000, 20, Side.Sell)
  val order5 = orderMsg(1180, 0, 13000, 60, Side.Sell)
  val order6 = orderMsg(1190, 0, 11000, 24, Side.Sell)
  val order7 = orderMsg(1220, 0, 10000, 50, Side.Sell)
  val order8 = orderMsg(2220, 0, 10000, 50, Side.Sell)

  val basic = {
    val basicSet = BasicSet(BasicSet.Config.default)
    AttributeSet(basicSet, Vector.empty[BasicSet => BasicAttribute[Double]])
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

  "LabeledPointsExtractor" should "extract valid states from orders log" in {
    val orders = Vector(order1, order2, order3, order4)
    val states = new extractor.RunWithOrders(orders)

    import states.{CurrentState, LabelState}

    val currentStates = states.currentStates.toVector
    assert(currentStates.size == 4)
    assert(currentStates(0) == CurrentState(order1, orderBook(order1), Vector(order1)))
    assert(currentStates(1) == CurrentState(order2, orderBook(order1, order2), Vector(order1, order2)))
    assert(currentStates(2) == CurrentState(order3, orderBook(order1, order2, order3), Vector(order1, order2, order3)))
    // First order should be removed from trail
    assert(currentStates(3) == CurrentState(order4, orderBook(order1, order2, order3, order4), Vector(order2, order3, order4)))

    val labelStates = states.labelStates.toVector
    assert(labelStates.size == 4)
    assert(labelStates(0) == LabelState(order1, orderBook(order1)))
    assert(labelStates(1) == LabelState(order2, orderBook(order1, order2)))
    assert(labelStates(2) == LabelState(order3, orderBook(order1, order2, order3)))
    assert(labelStates(3) == LabelState(order4, orderBook(order1, order2, order3, order4)))
  }

  it should "join states" in {
    val orders = Vector(order1, order2, order3, order4, order5, order6, order7, order8)
    val states = new extractor.RunWithOrders(orders)

    import states.{CurrentState, LabelState}

    val joinedStates = states.joinedState.toVector

    assert(joinedStates.size == 4)

    // Check pairs:  0 -> 1100, 100 -> 1200, 200 -> 1220, 1010 -> 2200

    assert(joinedStates(0)._1 == CurrentState(order1, orderBook(order1), Vector(order1)))
    assert(joinedStates(0)._2 == LabelState(order4, orderBook(order1, order2, order3, order4)))

    assert(joinedStates(1)._1 == CurrentState(order2, orderBook(order1, order2), Vector(order1, order2)))
    assert(joinedStates(1)._2 == LabelState(order5, orderBook(order1, order2, order3, order4, order5)))

    assert(joinedStates(2)._1 == CurrentState(order3, orderBook(order1, order2, order3), Vector(order1, order2, order3)))
    assert(joinedStates(2)._2 == LabelState(order7, orderBook(order1, order2, order3, order4, order5, order6, order7)))

    assert(joinedStates(3)._1 == CurrentState(order4, orderBook(order1, order2, order3, order4), Vector(order2, order3, order4)))
    assert(joinedStates(3)._2 == LabelState(order8, orderBook(order1, order2, order3, order4, order5, order6, order7, order8)))
  }
}
