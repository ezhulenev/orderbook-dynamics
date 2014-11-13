package com.scalafi.dynamics

import java.nio.file.{Files, Paths}

import com.scalafi.openbook.OpenBookMsg
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Codec

object OpenBook {
  private val log = LoggerFactory.getLogger(this.getClass)

  case class OpenBookFile(from: String, to: String, date: LocalDate, absolutePath: String)

  private[this] val openBookUltra = "openbookultra([A-Z]{2})_([A-Z]{1})([0-9]+)_1_of_1".r

  private[this] val dateFormat = DateTimeFormat.forPattern("YYYYMMdd")

  def openBookFiles(directory: String): Vector[OpenBookFile] = {
    val path = Paths.get(directory).toAbsolutePath
    assume(Files.isDirectory(path), s"Expected to see directory path: $directory")

    val files = Files.newDirectoryStream(path).
      iterator().asScala.map(_.toAbsolutePath.toString).
      map(absolutePath => (absolutePath, absolutePath.split("/").last)) collect {
      case (absolute, fileName @ openBookUltra(from, to, date)) =>
        OpenBookFile(from, to, dateFormat.parseLocalDate(date), absolute)
    }

    files.toVector
  }

  def orderLog(files: OpenBookFile*)(implicit sc: SparkContext): RDD[OpenBookMsg] = {
    val rdds = files map { file =>
      log.info(s"Load order log from Open Book file: ${file.absolutePath}")
      sc.parallelize(OpenBookMsg.iterate(file.absolutePath)(Codec.ISO8859).toSeq)
    }
    foldOrderLogs(rdds)
  }

  def orderLog(symbol: String, files: OpenBookFile*)(implicit sc: SparkContext): RDD[OpenBookMsg] = {
    val rdds = files map { file =>
      log.info(s"Load order log from Open Book file: ${file.absolutePath}")
      sc.parallelize(OpenBookMsg.iterate(file.absolutePath)(Codec.ISO8859).filter(_.symbol == symbol).toSeq)
    }
    foldOrderLogs(rdds)
  }

  private def foldOrderLogs(rdds: Iterable[RDD[OpenBookMsg]])(implicit sc: SparkContext): RDD[OpenBookMsg] =
    rdds.foldLeft(sc.emptyRDD[OpenBookMsg]: RDD[OpenBookMsg])(_ ++ _)

}