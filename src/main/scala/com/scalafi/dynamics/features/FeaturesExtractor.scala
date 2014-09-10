package com.scalafi.dynamics.features

import com.scalafi.openbook.OpenBookMsg
import com.scalafi.openbook.orderbook.BasicSet

import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.stream.process1._


object FeaturesExtractor {

  type FeaturesVector = (OpenBookMsg, Option[Int], Option[Int])

  trait Writes[T] {
    def apply(name: String): Sink[Task, T]
  }

  class WritesToMap[T] extends Writes[T] {
    private val collector = scala.collection.mutable.Map.empty[String, scala.collection.mutable.ListBuffer[T]]

    def apply(name: String): Sink[Task, T] = {
      val store = collector.getOrElseUpdate(name, scala.collection.mutable.ListBuffer.empty[T])
      scalaz.stream.io.channel(value => Task.now(store.append(value)))
    }

    def result = collector.mapValues(_.toVector).toMap
  }


  def extract(orders: Process[Task, OpenBookMsg])(implicit writes: Writes[FeaturesVector]) = {

    val extractFeatures: Process1[OpenBookMsg, Option[Process[Task, Unit]]] = {
      val processed = scala.collection.mutable.Set.empty[String]
      lift(order =>
        if (processed.contains(order.symbol)) None else {
          processed.add(order.symbol)
          Some(features(order.symbol, orders.filter(_.symbol == order.symbol)) to writes.apply(order.symbol))
        }
      )
    }

    scalaz.stream.merge.mergeN(orders.pipe(extractFeatures).filter(_.isDefined).map(_.get)).run
  }

  def features(symbol: String, orders: Process[Task, OpenBookMsg]) = {
    val basicSet = BasicSet(symbol, orders)
    val features = basicSet.askPrice(1) zip basicSet.bidPrice(1)
    orders.zipWith(features)((o, t) => (o, t._1, t._2))
  }

}