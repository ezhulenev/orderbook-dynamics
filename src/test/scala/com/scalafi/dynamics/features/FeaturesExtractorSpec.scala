package com.scalafi.dynamics.features


import com.scalafi.dynamics.features.FeaturesExtractor.{FeaturesVector, WritesToMap}
import com.scalafi.openbook.OpenBookMsg
import org.scalatest.FlatSpec

class FeaturesExtractorSpec extends FlatSpec {
  implicit val codec = io.Codec.ISO8859

  "Features Extractor" should "extract features for all presented symbols" in {

    val source = this.getClass.getResource("/openbookultraAA_N20130403_1_of_1").getPath

    val orders = OpenBookMsg.stream(source)

    implicit val writes = new WritesToMap[FeaturesVector]

    FeaturesExtractor.extract(orders).run

    writes.result foreach {
      case (key, messages) =>
        println(s"Key: $key. Features Vectors: ${messages.size}")
    }

    println(s"Total lines = ${writes.result.values.map(_.size).sum}")
  }
}
