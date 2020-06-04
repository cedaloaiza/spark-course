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
      val customerId = customer._1
      val totalPurchased = customer._2
      println(f"customer $customerId%s purshased $$$totalPurchased%.3f")
    }
    
    val totalCustumerSpentSorted = totalCustumerSpent.map(x => (x._2, x._1)).sortByKey()
    
    val totalCustumerSpentSortedScala = totalCustumerSpentSorted.collect()
    
    totalCustumerSpentSortedScala.foreach(println)
    
    
  }
}