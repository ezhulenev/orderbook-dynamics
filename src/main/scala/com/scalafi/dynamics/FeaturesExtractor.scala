package com.scalafi.dynamics

import com.scalafi.dynamics.attribute.{MeanPriceMove, MeanPriceMovementLabel, LabeledPointsExtractor}

trait FeaturesExtractor {

  import scala.concurrent.duration._

  // Include basic feature set
  def featuresExtractor(symbol: String): LabeledPointsExtractor[MeanPriceMove] = {
    import com.scalafi.dynamics.attribute.LabeledPointsExtractor._
    (LabeledPointsExtractor.newBuilder()
      // Basic Set Prices
      += basic(_.askPrice(1))
      += basic(_.askPrice(2))
      += basic(_.askPrice(3))
      += basic(_.askPrice(4))
      += basic(_.askPrice(5))
      += basic(_.bidPrice(1))
      += basic(_.bidPrice(2))
      += basic(_.bidPrice(3))
      += basic(_.bidPrice(4))
      += basic(_.bidPrice(5))
      // Basic Set Volumes
      += basic(_.askVolume(1))
      += basic(_.askVolume(2))
      += basic(_.askVolume(3))
      += basic(_.askVolume(4))
      += basic(_.askVolume(5))
      += basic(_.bidVolume(1))
      += basic(_.bidVolume(2))
      += basic(_.bidVolume(3))
      += basic(_.bidVolume(4))
      += basic(_.bidVolume(5))
      // Time Insensitive Set
      += timeInsensitive(_.meanAsk)
      += timeInsensitive(_.meanAskVolume)
      += timeInsensitive(_.meanBid)
      += timeInsensitive(_.meanBidVolume)
      += timeInsensitive(_.askStep(1))
      += timeInsensitive(_.askStep(2))
      += timeInsensitive(_.askStep(3))
      += timeInsensitive(_.askStep(4))
      += timeInsensitive(_.askStep(5))
      += timeInsensitive(_.bidStep(1))
      += timeInsensitive(_.bidStep(2))
      += timeInsensitive(_.bidStep(3))
      += timeInsensitive(_.bidStep(4))
      += timeInsensitive(_.bidStep(5))
      // Time Sensitive Set
      += timeSensitive(_.askArrivalRate)
      += timeSensitive(_.bidArrivalRate)
      ).result(symbol, MeanPriceMovementLabel, LabeledPointsExtractor.Config(1.second))
  }

}
