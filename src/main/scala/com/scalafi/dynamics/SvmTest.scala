package com.scalafi.dynamics

import com.scalafi.dynamics.svm.SVMOneVersusAll
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

import scala.util.Random

object SvmTest extends App with ConfiguredSparkContext {

  val rnd = new Random(seed = 123l)

  def labeledPoint(label: Int, mean: Int, deviation: Int, features: Int = 10): LabeledPoint = {
    def feature = (mean + (if (rnd.nextBoolean()) 1 else -1)*rnd.nextInt(deviation)).toDouble
    val featuresVector = Vectors.dense((0 until features).map(_ => feature).toArray)
    LabeledPoint(label, featuresVector)
  }

  val nFeatures = 50
  def label0() = labeledPoint(0, 10, 5, nFeatures)
  def label1() = labeledPoint(1, 100, 50, nFeatures)
  def label2() = labeledPoint(2, 1000, 500, nFeatures)

  val data0 = Seq.fill(1000)(label0())
  val data1 = Seq.fill(1000)(label1())
  val data2 = Seq.fill(1000)(label2())

  val rawData = sc.parallelize(data0 ++ data1 ++ data2)
  /*val scaler = new StandardScaler(true, true).fit(rawData.map(_.features))
  val scaled = rawData.map(p => p.copy(features = scaler.transform(p.features)))

  val splits =  scaled.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0).cache()

  val model = SVMOneVersusAll.train(training, 200)


  def verifySVM(label: String, gen: => LabeledPoint) = {
    val predictedMap = model.predict(scaler.transform(gen.features))
    val predicted = predictedMap.maxBy(_._2)._1
    println(s"Expected label = $label. Predicted = $predicted. Map = $predictedMap")
  }

  for (i <- 1 to 5) verifySVM("0", label0())
  for (i <- 1 to 5) verifySVM("1", label1())
  for (i <- 1 to 5) verifySVM("2", label2())*/


  // Train a DecisionTree model.
  //  Empty categoricalFeaturesInfo indicates all features are continuous.
  val numClasses = 3
  val categoricalFeaturesInfo = Map[Int, Int]()
  val impurity = "gini"
  val maxDepth = 5
  val maxBins = 100

  val dtModel = DecisionTree.trainClassifier(rawData, numClasses, categoricalFeaturesInfo, impurity,
    maxDepth, maxBins)

  def verifyDecisionTree(label: String, gen: => LabeledPoint) = {
    val predicted = dtModel.predict(gen.features)
    println(s"Expected label = $label. Predicted = $predicted")
  }

  for (i <- 1 to 5) verifyDecisionTree("0", label0())
  for (i <- 1 to 5) verifyDecisionTree("1", label1())
  for (i <- 1 to 5) verifyDecisionTree("2", label2())
  

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