package com.scalafi.dynamics.attribute

import com.scalafi.dynamics.attribute.LabeledPointsExtractor.AttributeSet
import com.scalafi.openbook.OpenBookMsg
import com.scalafi.openbook.orderbook.OrderBook
import framian.Cell
import org.apache.spark.mllib.regression.LabeledPoint

import scala.concurrent.duration._

object LabeledPointsExtractorBuilder {
  def newBuilder: LabeledPointsExtractorBuilder = ???
}

trait LabeledPointsExtractorBuilder {
  def add[T: Numeric](attribute: BasicAttribute[T]): this.type

  def add[T: Numeric](attribute: TimeInsensitiveAttribute[T]): this.type

  def add[T: Numeric](attribute: TimeSensitiveAttribute[T]): this.type

  def +=[T: Numeric](attribute: BasicAttribute[T]): this.type = add(attribute)

  def +=[T: Numeric](attribute: TimeInsensitiveAttribute[T]): this.type = add(attribute)

  def +=[T: Numeric](attribute: TimeSensitiveAttribute[T]): this.type = add(attribute)

  def result[L: LabelEncode](symbol: String, label: Label[L], config: LabeledPointsExtractor.Config = LabeledPointsExtractor.Config.default): LabeledPointsExtractor[L]
}

object LabeledPointsExtractor {

  case class AttributeSet[S, A](set: S, attributes: Vector[S => A])

  trait Config {
    def labelDuration: Duration
  }

  object Config {
    val default = new Config {
      val labelDuration: Duration = 1.second
    }
  }

}

class LabeledPointsExtractor[L: LabelEncode] private[attribute](symbol: String,
                                                                label: Label[L],
                                                                config: LabeledPointsExtractor.Config = LabeledPointsExtractor.Config.default)
                                                               (basicSet:           AttributeSet[BasicSet,           BasicAttribute[Double]])
                                                               (timeInsensitiveSet: AttributeSet[TimeInsensitiveSet, TimeInsensitiveAttribute[Double]])
                                                               (timeSensitiveSet:   AttributeSet[TimeSensitiveSet,   TimeSensitiveAttribute[Double]]) {

  type OrderBookAttribute = OrderBook => Cell[Double]

  def labeledPoints(orders: Vector[OpenBookMsg]): Vector[LabeledPoint] = {
    var currentOrderBook: OrderBook = OrderBook.apply(symbol)
    var labelOrderBook: OrderBook = OrderBook.apply(symbol)


    Vector.empty
  }

  class RunWithOrders(openBookMessages: Vector[OpenBookMsg]) { self =>

    /*
     Two iterators, label-iterator is 'labelDuration' ahead current-iterator

     Time:        | > > >  | > > > > > > > > > > > > > > | > > > >  | > > > > > > > > >
                  | 0sec   |    +1sec     +2sec     +3sec  |    +4sec  |    +5sec     +6sec
     Orders:      | order1 | -> order2 -> order3 -> order4 | -> order5 | -> order6 -> order7
     Order Books: | book1  | -> book2  -> book3  -> book4  | -> book5  | -> book6  -> book7
                   -------                                  ----------
     State             ^ - current state                         ^ -> label state
    */


    case class CurrentState(order: OpenBookMsg, orderBook: OrderBook, trail: Vector[OpenBookMsg])

    case class LabelState(order: OpenBookMsg, orderBook: OrderBook)

    def currentStates: Iterator[CurrentState] = {
      (orders zip orderBooks zip trails) map {
        case ((order, orderBook), trail) => CurrentState(order, orderBook, trail)
      }
    }

    def labelStates: Iterator[LabelState] = {
      (orders zip orderBooks) map {
        case (order, orderBook) => LabelState(order, orderBook)
      }
    }

    def joinedState: Iterator[(CurrentState, LabelState)] = {
      val dropDuration = config.labelDuration.toMillis

      // Create local copies of iterators
      val current = self.currentStates
      var label = self.labelStates

      current.map { currentState =>
        label = label.dropWhile(_.order.sourceTime - currentState.order.sourceTime < dropDuration)
        if (label.hasNext) (currentState, Some(label.next())) else (currentState, None)
      } collect {
        case (currentState, Some(labelState)) => (currentState, labelState)
      }
    }

    private def orders: Iterator[OpenBookMsg] =
      openBookMessages.iterator

    private def orderBooks: Iterator[OrderBook] =
      openBookMessages.iterator.scanLeft(OrderBook.empty(symbol))((ob, o) => ob.update(o)).drop(1)

    def trails: Iterator[Vector[OpenBookMsg]] =
      openBookMessages.iterator.scanLeft(Vector.empty[OpenBookMsg])((t, o) => timeSensitiveSet.set.trail(t, o)).drop(1)
  }
}