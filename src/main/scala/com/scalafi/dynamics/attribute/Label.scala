package com.scalafi.dynamics.attribute

import com.scalafi.openbook.orderbook.OrderBook
import framian.Cell

sealed trait Label[L] {
  def apply(current: OrderBook, future: OrderBook): Option[L]
}

sealed trait MeanPriceMove

object MeanPriceMove {
  case object Up extends MeanPriceMove
  case object Down extends MeanPriceMove
  case object Stationary extends MeanPriceMove
}

object MeanPriceMovementLabel extends Label[MeanPriceMove] {

  private[this] val basicSet = BasicSet.apply(BasicSet.Config.default)


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