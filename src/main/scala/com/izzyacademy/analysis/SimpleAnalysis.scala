package com.izzyacademy.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleAnalysis {

  def main(args: Array[String]): Unit = {

    val dataLocation = "/home/iekpo/spark-data/users.csv"

    val sparkConfig = new SparkConf().setMaster("local[*]").set("spark.driver.memory", "4g").setAppName("IsraelAnalysis")

    val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

    val df = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(dataLocation)

    df.show()

    sparkSession.stop()

  }

}
