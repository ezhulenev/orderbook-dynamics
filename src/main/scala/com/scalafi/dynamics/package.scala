package com.scalafi

import com.scalafi.openbook.OpenBookMsg
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

package object dynamics {

  implicit def toOrderLogFunctions(orderLog: RDD[OpenBookMsg]): OrderLogFunctions =
    new OrderLogFunctions(orderLog)

}
