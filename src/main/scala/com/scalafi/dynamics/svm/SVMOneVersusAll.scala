package com.scalafi.dynamics.svm

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

class SVMOneVersusAllModel(val models: Map[Double, SVMModel]) {
  def predict(testData: Vector): Map[Double, Double] = {
    models.mapValues(_.predict(testData))
  }
}

class SVMOneVersusAll(svm: SVMWithSGD) {
  private val log = LoggerFactory.getLogger(classOf[SVMOneVersusAll])

  /**
   * Relabel input RDD. Set points labeled as @label to 1.0 and all other to 0.0
   */
  private def relabel(input: RDD[LabeledPoint], label: Double): RDD[LabeledPoint] = {
    input.map(point => if (point.label == label) point.copy(label = 1.0) else point.copy(label = 0.0))
  }

  def run(input: RDD[LabeledPoint]): SVMOneVersusAllModel = {
    val labels = input.map(_.label).collect().toSet

    log.info(s"Input labels number: ${labels.size}. Labels: [${labels.mkString(", ")}]")
    assume(labels.size > 2, s"Labels size should be greater than 2")

    val models = labels.map { label =>
      log.debug(s"Train model: '$label' versus [${labels.filterNot(_ == label).mkString(", ")}]")
      val model = svm.run(relabel(input, label))
      model.clearThreshold()
      (label, model)
    }

    new SVMOneVersusAllModel(models.toMap)
  }
}

object SVMOneVersusAll {
  def  train(input: RDD[LabeledPoint], numIterations: Int): SVMOneVersusAllModel = {
    val svmAlg = new SVMWithSGD()

    svmAlg.optimizer.
      setNumIterations(numIterations)

    new SVMOneVersusAll(svmAlg).run(input)
  }
}