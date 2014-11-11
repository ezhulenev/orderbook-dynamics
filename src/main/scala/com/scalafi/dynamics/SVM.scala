package com.scalafi.dynamics

import org.apache.spark.mllib.feature.StandardScaler

object SVM extends App {

  import org.apache.spark.mllib.classification.SVMWithSGD
  import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.{SparkContext, SparkConf}

  import scala.util.Random

  private lazy val sparkConf =
    new SparkConf()
      .setMaster("local[2]").
      setAppName("SparkSVM")

  lazy val sc = new SparkContext(sparkConf)

  val rnd = new Random(seed = 123l)

  def labeledPoint(label: Int, mean: Int, deviation: Int, features: Int = 10): LabeledPoint = {
    def feature = (mean + (if (rnd.nextBoolean()) 1 else -1)*rnd.nextInt(deviation)).toDouble
    val featuresVector = Vectors.dense((0 until features).map(_ => feature).toArray)
    LabeledPoint(label, featuresVector)
  }

  val nFeatures = 2
  def label0 = labeledPoint(0, 10, 5, features = 2)
  def label1 = labeledPoint(1, 100, 10, features = 2)

  val data0 = Seq.fill(100)(label0)
  val data1 = Seq.fill(200)(label1)

  val data = sc.parallelize(data0 ++ data1)

  val scaler = new StandardScaler(true, true).fit(data.map(_.features))
  val scaled = data.map(point => point.copy(features = scaler.transform(point.features)))

  // Split data into training (60%) and test (40%).
  val splits =  scaled.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0).cache()
  val test = splits(1)

  // Run training algorithm to build the model
  val numIterations = 300
  val svm = new SVMWithSGD()
  svm.optimizer.
    setNumIterations(numIterations)
  val model = svm.run(training)

  // Predict
  Seq.fill(10)(label0).map {
    point =>
      println(s"Label0 ${model.predict(scaler.transform(point.features))}")
  }

  Seq.fill(10)(label1).map {
    point =>
      println(s"Label1 ${model.predict(scaler.transform(point.features))}")
  }

  // Clear the default threshold.
  model.clearThreshold()

  // Compute raw scores on the test set.
  val scoreAndLabels = test.map { point =>
    val score = model.predict(point.features)
    (score, point.label)
  }

  // Get evaluation metrics.
  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
  val auROC = metrics.areaUnderROC()

  println(s"Area under ROC = $auROC")
}
