package com.scalafi.dynamics

import com.scalafi.dynamics.attribute.{LabeledPointsExtractor, MeanPriceMovementLabel}
import com.scalafi.openbook.OpenBookMsg
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

import scala.io.Codec

object Extractor {
  def extractor(symbol: String) = {
    import com.scalafi.dynamics.attribute.LabeledPointsExtractor._
    (LabeledPointsExtractor.newBuilder()
      += basic(_.askPrice(1))
      += basic(_.bidPrice(1))
      += basic(_.meanPrice)
      ).result(symbol, MeanPriceMovementLabel, LabeledPointsExtractor.Config(1.millisecond))
  }
}

class OrderLogFunctionsSpec extends FlatSpec with ConfiguredSparkContext {
  private val log = LoggerFactory.getLogger(classOf[OrderLogFunctionsSpec])

  lazy val orderLog = {
    implicit val codec = Codec.ISO8859
    val orders = OpenBookMsg.iterate(this.getClass.getResourceAsStream("/openbookultraAA_N20130403_1_of_1"))
    sc.parallelize(orders.toSeq)
  }

  val Symbol = "ANR"

  "OrderLogFunctions" should "get all symbols from order log" in {
    val symbols = orderLog.symbols()
    assert(symbols contains "ANR")
  }

  it should "count orders by symbol" in {
    val count = orderLog.countBySymbol()
    assert(count(Symbol) == 79)
    assert(count.toSeq.map(_._2).sum == orderLog.count())
  }
  
  it should "extract labeled data by symbol" in {
    val labeledOrderLog = orderLog.extractLabeledData(Symbol)(Extractor.extractor)
    assert(labeledOrderLog.size == 1)

    val ANROrderLog = labeledOrderLog.head
    assert(ANROrderLog.symbol == Symbol)

    val data = ANROrderLog.labeledPoints.collect().toVector
    log.info(s"Labeled data size = ${data.size}")
    // (Ask + Bid) / 2 == MeanPrice
    assert(data.size > 50)
    data foreach { labeledPoint =>
      val features = labeledPoint.features.toArray
      assert((features(0) + features(1)) / 2.0 == features(2))
    }
  }

}
