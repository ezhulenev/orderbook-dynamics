package com.scalafi.dynamics

import java.nio.file.{Files, Paths}

import com.scalafi.dynamics.OpenBook.OpenBookFile
import com.scalafi.dynamics.attribute.{LabelEncode, MeanPriceMove}
import com.scalafi.openbook.OpenBookMsg
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scopt.OptionParser

object DecisionTreeDynamics extends App with ConfiguredSparkContext with FeaturesExtractor {
  private val log = LoggerFactory.getLogger(this.getClass)

  case class Config(training: String = "",
                    validation: String = "",
                    filter: Option[String] = None,
                    symbol: Option[String] = None)

  val parser = new OptionParser[Config]("Order Book Dynamics") {
    head("Order Book Dynamics")

    def isDirectory(s: String) = if (Files.isDirectory(Paths.get(s))) success else failure(s"Path is not directory: $s")

    opt[String]('t', "training") action { (x, c) =>
      c.copy(training = x)
    } text "OpenBook: path to training data" required() validate isDirectory

    opt[String]('v', "validation") action { (x, c) =>
      c.copy(validation = x)
    } text "OpenBook: path to validation data" required() validate isDirectory

    opt[String]('f', "filter") action { (x, c) =>
      c.copy(filter = Some(x))
    } text "OpenBook: filter by prefix ('AA', 'AZ', etc ...)" optional()

    opt[String]('s', "symbol") action { (x, c) =>
      c.copy(symbol = Some(x))
    } text "OpenBook: filter by symbol " optional()
  }

  parser.parse(args, Config()) map { implicit config =>
    val trainingFiles = openBookFiles("Training", config.training, config.filter)
    val validationFiles = openBookFiles("Validation", config.validation, config.filter)

    val trainingOrderLog = orderLog(trainingFiles)
    log.info(s"Training order log size: ${trainingOrderLog.count()}")

    // Configure DecisionTree model
    val labelEncode = implicitly[LabelEncode[MeanPriceMove]]
    val numClasses = labelEncode.numClasses
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 100

    val trainingData = trainingOrderLog.extractLabeledData(featuresExtractor(_: String))
    val trainedModels = (trainingData map { case LabeledOrderLog(symbol, labeledPoints) =>
      log.info(s"$symbol: Train Decision Tree model. Training data size: ${labeledPoints.count()}")
      val start = System.currentTimeMillis()
      val model = DecisionTree.trainClassifier(labeledPoints, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
      val end = System.currentTimeMillis()
      log.info(s"$symbol: Trained model in ${end - start} millis")
      val labelCounts = labeledPoints.map(_.label).countByValue().map {
        case (key, count) => (labelEncode.decode(key.toInt), count)
      }
      log.info(s"$symbol: Label counts: [${labelCounts.mkString(", ")}]")
      symbol -> model
    }).toMap

    val validationOrderLog = orderLog(validationFiles)
    log.info(s"Validation order log size: ${validationOrderLog.count()}")
    val validationData = validationOrderLog.extractLabeledData(featuresExtractor(_: String))

    // Evaluate model on validation data and compute training error
    validationData.map { case LabeledOrderLog(symbol, labeledPoints) =>

      val model = trainedModels(symbol)

      log.info(s"$symbol: Evaluate model on validation data. Validation data size: ${labeledPoints.count()}")
      log.info(s"$symbol: Learned classification tree model: $model")

      val labelAndPrediction = labeledPoints.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val trainingError = labelAndPrediction.filter(r => r._1 != r._2).count().toDouble / labeledPoints.count
      log.info(s"$symbol: Training Error = " + trainingError)
    }
  }

  private def orderLog(files: Vector[OpenBookFile])(implicit config: Config): RDD[OpenBookMsg] =
    config.symbol match {
    case None => OpenBook.orderLog(files:_*)
    case Some(symbol) => OpenBook.orderLog(symbol, files:_*)
  }

  private def openBookFiles(name: String, path: String, filter: Option[String]) = {
    log.info(s"Load $name data from: $path. Filtered: ${filter.getOrElse("no")}")
    val files = {
      val allFiles = OpenBook.openBookFiles(path)
      filter match {
        case None => allFiles
        case Some(prefix) => allFiles.filter(_.from == prefix)
      }
    }
    log.debug(s"$name data set [${files.size}]:")
    files.foreach(file => log.debug(s" - $file"))
    files
  }
}