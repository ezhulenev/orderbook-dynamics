package com.scalafi.dynamics

import com.scalafi.openbook._


package object attribute {
  val Symbol = "AAPL"

  def orderMsg(sourceTime: Int, sourceTimeMicroSecs: Short, price: Int, volume: Int, side: Side) =
    OpenBookMsg(
      msgSeqNum = 0,
      msgType = MsgType.DeltaUpdate,
      sendTime = 0,
      symbol = Symbol,
      msgSize = 46,
      securityIndex = 0,
      sourceTime = sourceTime + 300000,
      sourceTimeMicroSecs = sourceTimeMicroSecs,
      quoteCondition = QuoteCondition.Normal,
      tradingStatus = TradingStatus.Opened,
      sourceSeqNum = 0,
      sourceSessionId = 0,
      priceScaleCode = 4,
      priceNumerator = price,
      volume = volume,
      chgQty = 0,
      numOrders = 0,
      side = side,
      reasonCode = ReasonCode.Order,
      linkID1 = 0,
      linkID2 = 0,
      linkID3 = 0)

}
