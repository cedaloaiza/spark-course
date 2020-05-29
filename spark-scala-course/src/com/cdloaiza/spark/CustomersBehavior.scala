package com.cdloaiza.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomersBehavior {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "CustomersBehavior")
    
    val input = sc.textFile("../data/customer-orders.csv")
    
    val customerSpent = input.map(x => { val splited = x.split(","); (splited(0), splited(2).toDouble) })
    
    val totalCustumerSpent = customerSpent.reduceByKey((x, y) => x + y)
    
    val totalCustumerSpentScala = totalCustumerSpent.collect()
    
    for (customer <- totalCustumerSpentScala) {
      println(customer._1 + " customer " + customer._2)
    }
    
    
  }
}