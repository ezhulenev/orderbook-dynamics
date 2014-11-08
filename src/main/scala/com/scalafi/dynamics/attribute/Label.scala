package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook
import framian.Cell

trait Label[L] { label =>

  def values: Vector[L]

  def apply(current: OrderBook, future: OrderBook): Option[L]

  def encode(implicit labelCode: LabelEncode[L]): Label[Int] = new Label[Int] {
    val values: Vector[Int] = label.values.map(labelCode.code)
    def apply(current: OrderBook, future: OrderBook): Option[Int] =
      label.apply(current, future).map(labelCode.code)
  }
}

trait LabelEncode[L] {
  def code(label: L): Int
}

sealed trait MeanPriceMove

object MeanPriceMove {
  case object Up extends MeanPriceMove
  case object Down extends MeanPriceMove
  case object Stationary extends MeanPriceMove

  implicit object MeanPriceMoveEncode extends LabelEncode[MeanPriceMove] {
    def code(label: MeanPriceMove): Int = label match {
      case Up => 0
      case Down => 1
      case Stationary => 2
    }
  }
}

object MeanPriceMovementLabel extends Label[MeanPriceMove] {

  private[this] val basicSet = BasicSet.apply(BasicSet.Config.default)

  val values: Vector[MeanPriceMove] = Vector(MeanPriceMove.Up, MeanPriceMove.Down, MeanPriceMove.Stationary)

  def apply(current: OrderBook, future: OrderBook): Option[MeanPriceMove] = {
    val currentMeanPrice = basicSet.meanPrice(current)
    val futureMeanPrice = basicSet.meanPrice(future)

    val cell: Cell[MeanPriceMove] = currentMeanPrice.zipMap(futureMeanPrice) { (currentMeanValue, futureMeanValue) =>
      if (currentMeanValue == futureMeanValue)
        MeanPriceMove.Stationary
      else if (currentMeanValue > futureMeanValue)
        MeanPriceMove.Down
      else
        MeanPriceMove.Up
    }

    cell.toOption
  }
}