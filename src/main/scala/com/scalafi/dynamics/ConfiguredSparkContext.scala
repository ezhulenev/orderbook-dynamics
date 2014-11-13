package com.scalafi.dynamics

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

trait ConfiguredSparkContext {
  private val log = LoggerFactory.getLogger(classOf[ConfiguredSparkContext])

  private val config = ConfigFactory.load()

  private lazy val sparkConf = {
    val master = config.getString("spark.master")
    val appName = config.getString("spark.app-name")

    log.info(s"Create spark context. Master: $master. App Name: $appName")
    new SparkConf().
      setMaster(master).
      setAppName(appName).
      setJars(SparkContext.jarOfClass(this.getClass).toSeq)
  }

  implicit lazy val sc = new SparkContext(sparkConf)
}