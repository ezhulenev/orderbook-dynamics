package com.scalafi.dynamics

import com.scalafi.dynamics.svm.SVMOneVersusAll
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random

object SvmTest extends App with ConfiguredSparkContext {

  def labeledPoint(label: Int, center: Int, max: Int, features: Int = 10): LabeledPoint = {
    val rnd = new Random()
    def nextFeature() = {
      val n = rnd.nextInt(max)
      (center + n).toDouble
    }
    val featuresVector = Vectors.sparse(features, for (i <- 0 until features /*filter(_ => rnd.nextBoolean())*/) yield (i, nextFeature()))
    LabeledPoint(label, featuresVector)
  }


  def assignTo0(point: LabeledPoint): LabeledPoint = point.copy(label = 0)
  def assignTo1(point: LabeledPoint): LabeledPoint = point.copy(label = 1)

  def label0() = labeledPoint(0, 10, 5, features = 2)
  def label1() = labeledPoint(1, 20, 5, features = 2)
  def label2() = labeledPoint(2, 30, 5, features = 2)

  val data0 = Seq.fill(50)(label0())
  val data1 = Seq.fill(50)(label1())
  val data2 = Seq.fill(50)(label2())

  val input = sc.parallelize(data0 ++ data1 ++ data2)

  val model = SVMOneVersusAll.train(input, 100)


  for (i <- 1 to 50) println("Label0 = " + model.predict(label0().features))
  for (i <- 1 to 50) println("Label1 = " + model.predict(label1().features))
  for (i <- 1 to 50) println("Label2 = " + model.predict(label2().features))

  /*val data = sc.parallelize(Seq.fill(1000)(label0()) ++ Seq.fill(1000)(label1()))

  // Split data into training (60%) and test (40%).
  val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0).cache()
  val test = splits(1)

  // Run training algorithm to build the model
  val numIterations = 300
  val model = SVMWithSGD.train(training, numIterations)

  println(s"MODEL = $model")


  // Clear the default threshold.
  model.clearThreshold()

  // Compute raw scores on the test set.
  val scoreAndLabels = test.map { point =>
    val score = model.predict(point.features)
    (score, point.label)
  }

  scoreAndLabels.foreach(println)

  // Get evaluation metrics.
  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
  val auROC = metrics.areaUnderROC()

  metrics.fMeasureByThreshold().collect().foreach {
    case (threshold, fMeasure) => println(s"Threshold = $threshold. F Measure = $fMeasure")
  }

  println("Area under ROC = " + auROC)

  println(label0())
  println(label1())*/
}