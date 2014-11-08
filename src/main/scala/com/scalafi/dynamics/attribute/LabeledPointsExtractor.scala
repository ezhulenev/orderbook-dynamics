package com.scalafi.dynamics.attribute

import com.scalafi.dynamics.attribute.LabeledPointsExtractor.Config
import com.scalafi.openbook.OpenBookMsg
import com.scalafi.openbook.orderbook.OrderBook
import framian.Cell
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

private[this] case class AttributeSet[S, A](set: S, attributes: Vector[S => A])

object LabeledPointsExtractorBuilder {

  def newBuilder(basicConfig: BasicSet.Config = BasicSet.Config.default,
                 timeInsensitiveConfig: TimeInsensitiveSet.Config = TimeInsensitiveSet.Config.default,
                 timeSensitiveConfig: TimeSensitiveSet.Config = TimeSensitiveSet.Config.default
                 ): LabeledPointsExtractorBuilder =
    new GenericLabeledPointsExtractorBuilder(basicConfig, timeInsensitiveConfig, timeSensitiveConfig)

  case class ReadBasicAttribute(f: BasicSet => BasicAttribute[Double])
  case class ReadTimeInsensitiveAttribute(f: TimeInsensitiveSet => TimeInsensitiveAttribute[Double])
  case class ReadTimeSensitiveAttribute(f: TimeSensitiveSet => TimeSensitiveAttribute[Double])

  def basic[T: Numeric](f: BasicSet => BasicAttribute[T]): ReadBasicAttribute = {
    ReadBasicAttribute((set: BasicSet) => f(set).map(implicitly[Numeric[T]].toDouble))
  }

  def timeInsensitive[T: Numeric](f: TimeInsensitiveSet => TimeInsensitiveAttribute[T]): ReadTimeInsensitiveAttribute = {
    ReadTimeInsensitiveAttribute((set: TimeInsensitiveSet) => f(set).map(implicitly[Numeric[T]].toDouble))
  }

  def timeSensitive[T: Numeric](f: TimeSensitiveSet => TimeSensitiveAttribute[T]): ReadTimeSensitiveAttribute = {
    ReadTimeSensitiveAttribute((set: TimeSensitiveSet) => f(set).map(implicitly[Numeric[T]].toDouble))
  }

  private[LabeledPointsExtractorBuilder] class GenericLabeledPointsExtractorBuilder
                                               (basic: BasicSet.Config,
                                                timeInsensitive: TimeInsensitiveSet.Config,
                                                timeSensitive: TimeSensitiveSet.Config
                                               ) extends LabeledPointsExtractorBuilder {

    import scala.collection.mutable

    private val basicAttributes =
      mutable.ListBuffer.empty[BasicSet => BasicAttribute[Double]]

    private val timeInsensitiveAttributes =
      mutable.ListBuffer.empty[TimeInsensitiveSet => TimeInsensitiveAttribute[Double]]

    private val timeSensitiveAttributes =
      mutable.ListBuffer.empty[TimeSensitiveSet => TimeSensitiveAttribute[Double]]

    def add(attribute: ReadBasicAttribute) = { basicAttributes += attribute.f; this }

    def add(attribute: ReadTimeInsensitiveAttribute) = { timeInsensitiveAttributes += attribute.f; this }

    def add(attribute: ReadTimeSensitiveAttribute) = { timeSensitiveAttributes += attribute.f; this }

    def result[L: LabelEncode](symbol: String, label: Label[L],
                               config: Config = Config.default): LabeledPointsExtractor[L] = {

      val basicSet = AttributeSet(BasicSet(basic), basicAttributes.toVector)
      val timeInsensitiveSet = AttributeSet(TimeInsensitiveSet(timeInsensitive), timeInsensitiveAttributes.toVector)
      val timeSensitiveSet = AttributeSet(TimeSensitiveSet(timeSensitive), timeSensitiveAttributes.toVector)
      new LabeledPointsExtractor[L](symbol, label, config)(basicSet)(timeInsensitiveSet)(timeSensitiveSet)
    }
  }
}

trait LabeledPointsExtractorBuilder {

  import com.scalafi.dynamics.attribute.LabeledPointsExtractor.Config
  import com.scalafi.dynamics.attribute.LabeledPointsExtractorBuilder._

  def add(attribute: ReadBasicAttribute): this.type

  def add(attribute: ReadTimeInsensitiveAttribute): this.type

  def add(attribute: ReadTimeSensitiveAttribute): this.type

  def +=(attribute: ReadBasicAttribute): this.type = add(attribute)

  def +=(attribute: ReadTimeInsensitiveAttribute): this.type = add(attribute)

  def +=(attribute: ReadTimeSensitiveAttribute): this.type = add(attribute)

  def result[L: LabelEncode](symbol: String, label: Label[L],
                             config: Config = Config.default): LabeledPointsExtractor[L]
}


object LabeledPointsExtractor {

  trait Config {
    def labelDuration: Duration
  }

  object Config {
    val default = new Config {
      val labelDuration: Duration = 1.second
    }
  }

}

class LabeledPointsExtractor[L: LabelEncode] private[attribute]
      (symbol: String,
       label: Label[L],
       config: LabeledPointsExtractor.Config = LabeledPointsExtractor.Config.default
      )
      (basicSet:           AttributeSet[BasicSet,           BasicAttribute[Double]])
      (timeInsensitiveSet: AttributeSet[TimeInsensitiveSet, TimeInsensitiveAttribute[Double]])
      (timeSensitiveSet:   AttributeSet[TimeSensitiveSet,   TimeSensitiveAttribute[Double]]) {

  private val log = LoggerFactory.getLogger(classOf[LabeledPointsExtractor[L]])

  type FeatureExtractor = AttributesCursor => Cell[Double]
  type LabelExtractor = (AttributesCursor, LabelCursor) => Option[Int]
  
  private val featureExtractors: Vector[(Int, FeatureExtractor)] =
    (basicSet.attributes.
       map(attr => attr(basicSet.set)).
       map(attr => (cursor: AttributesCursor) => attr(cursor.orderBook)) ++
     timeInsensitiveSet.attributes.
       map(attr => attr(timeInsensitiveSet.set)).
       map(attr => (cursor: AttributesCursor) => attr(cursor.orderBook)) ++
     timeSensitiveSet.attributes.
       map(attr => attr(timeSensitiveSet.set)).
       map(attr => (cursor: AttributesCursor) => attr(cursor.trail))
    ).zipWithIndex.map(_.swap)

  private val labelExtractor: LabelExtractor = {
    case (attr, lbl) => label.encode.apply(attr.orderBook, lbl.orderBook)
  }

  log.info(s"Build labeled points extractor for symbol: $symbol. " +
    s"Number of features: ${featureExtractors.size}")

  def labeledPoints(orders: Vector[OpenBookMsg]): Vector[LabeledPoint] = {
    log.info(s"Extract labeled points from orders log. Log size: ${orders.size}")

    val traversal = new OrdersCursorTraversal(orders)
    val labeledPoints: Iterator[LabeledPoint] =
      traversal.cursor map { case (attributesCursor, labelCursor) =>
        val values = featureExtractors.map {
          case (idx, extractor) => (idx, extractor(attributesCursor))
        } collect {
          case (idx, cell) if cell.isValue => (idx, cell.get)
        }
        val labelCode = labelExtractor(attributesCursor, labelCursor)
        (labelCode, values)
      } collect {
        case (Some(labelCode), values) if values.nonEmpty =>
          val featureVector = Vectors.sparse(featureExtractors.size, values)
          LabeledPoint(labelCode.toDouble, featureVector)
      }

    labeledPoints.toVector
  }

  /*
    Two cursors: attributes-cursor + label-cursor 'labelDuration' ahead of it
    Feature vector calculated from 'attributes-cursor'
    Label is based on 'label-cursor'
   
    Time:        | > > >  | > > > > > > > > > > > > > > > | > > > > > | > > > > > > > > > > >
                 | 0sec   |    +1sec     +2sec     +3sec  |    +4sec  |    +5sec     +6sec
    Orders:      | order1 | -> order2 -> order3 -> order4 | -> order5 | -> order6 -> order7
    Order Books: | book1  | -> book2  -> book3  -> book4  | -> book5  | -> book6  -> book7
                  -------                                  ----------
    Cursors:          ^ - attributes cursor                     ^ -> label cursor

                      >-----------------------------------------<
                               label duration = 4 seconds
  */


  case class AttributesCursor(order: OpenBookMsg, orderBook: OrderBook, trail: Vector[OpenBookMsg])

  case class LabelCursor(order: OpenBookMsg, orderBook: OrderBook)

  class OrdersCursorTraversal(openBookMessages: Vector[OpenBookMsg]) { self =>
    
    def cursor: Iterator[(AttributesCursor, LabelCursor)] = {
      val labelLookForwardMillis = config.labelDuration.toMillis

      // Create local copies of iterators
      val attributes = self.attributes
      var labels = self.labels

      attributes.map { attribute =>
        labels = labels.dropWhile(_.order.sourceTime - attribute.order.sourceTime < labelLookForwardMillis)
        if (labels.hasNext) (attribute, Some(labels.next())) else (attribute, None)
      } collect { case (attr, Some(lbl)) => (attr, lbl) }
    }

    def attributes: Iterator[AttributesCursor] =
      (orders zip orderBooks zip trails) map {
        case ((order, orderBook), trail) => AttributesCursor(order, orderBook, trail)
      }

    def labels: Iterator[LabelCursor] =
      (orders zip orderBooks) map {
        case (order, orderBook) => LabelCursor(order, orderBook)
      }

    private def orders: Iterator[OpenBookMsg] =
      openBookMessages.iterator

    private def orderBooks: Iterator[OrderBook] =
      orders.scanLeft(OrderBook.empty(symbol))((ob, o) => ob.update(o)).drop(1)

    def trails: Iterator[Vector[OpenBookMsg]] =
      orders.scanLeft(Vector.empty[OpenBookMsg])((t, o) => timeSensitiveSet.set.trail(t, o)).drop(1)
  }

}