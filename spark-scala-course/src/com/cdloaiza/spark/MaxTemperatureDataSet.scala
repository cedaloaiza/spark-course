package com.cdloaiza.spark

import org.apache.spark.sql.SparkSession

object MaxTemperatureDataSet {
  
  case class Sample(station:String, date:Int, tag: String, temperature: Double)
  
  def getTemperatures (line: String) : Sample = {
    val values = line.split(",")
    val temp = values(3).toDouble * 0.1
    return Sample(values(0), values(1).toInt, values(2), temp)
  } 
  
  def main (args: Array[String]) {
    
    val ss = SparkSession.builder().appName("maxTemp").master("local[*]").getOrCreate()
    
    val rawTemps = ss.sparkContext.textFile("../data/1800.csv")
    
    // Convert to a DataSet
    import ss.implicits._
    
    val tempsDS = rawTemps.map(getTemperatures).toDS()
    
    tempsDS.createOrReplaceTempView("temperatures")
    
    val maxTemp = ss.sql("SELECT max(temperature) FROM temperatures WHERE tag='TMAX'")
    
    maxTemp.show()
    
    
    
    
  }
  
}