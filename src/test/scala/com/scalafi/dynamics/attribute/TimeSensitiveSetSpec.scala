package com.scalafi.dynamics.attribute

import com.scalafi.openbook.{OpenBookMsg, Side}
import org.scalatest.FlatSpec

import scala.concurrent.duration._

class TimeSensitiveSetSpec extends FlatSpec {

  val order1 = orderMsg(0, 0, 10000, 10, Side.Buy)
  val order2 = orderMsg(100, 0, 10000, 15, Side.Buy)
  val order3 = orderMsg(200, 0, 11000, 20, Side.Sell)
  val order4 = orderMsg(300, 0, 12000, 30, Side.Sell)
  val order5 = orderMsg(400, 0, 14000, 40, Side.Sell)
  val order6 = orderMsg(800, 0, 14000, 40, Side.Sell)
  val order7 = orderMsg(900, 0, 14000, 40, Side.Sell)
  val order8 = orderMsg(1100, 0, 14000, 40, Side.Sell)

  val orders = Vector(order1, order2, order3, order4, order5, order6, order7, order8)

  val config = TimeSensitiveSet.Config(1.second)

  val timeSensitiveSet = TimeSensitiveSet(config)

  val trail = orders.foldLeft(Vector.empty[OpenBookMsg]) {
    (trail, order) => timeSensitiveSet.trail(trail, order)
  }

  "TimeSensitiveSet features" should "build valid orders trail" in {
    assert(trail.head == order2)
    assert(trail.last == order8)
  }

  it should "build valid ask arrival rate" in {
    val askArrivalRate = timeSensitiveSet.askArrivalRate
    assert(askArrivalRate(trail) == Some(6.0 / 1000)) // 6 sell orders in 1000 millis
  }
}
