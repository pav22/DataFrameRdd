package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import example.common.ScalaData

object DataFrameMain {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("DataFrameRDD").setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    val sqlCon = new SQLContext(sc)
    
    val dataFrame = sqlCon.createDataFrame(ScalaData.sampleData())
    
    print("Selecting age < 20")
    dataFrame.filter("age < 20").collect.foreach(x => println(x))
   
    
    print("Selecting age > 20")
    dataFrame.filter(dataFrame.col("age").gt(20)).collect.foreach(x=>println(x))
    
   
    val rdd = sc.parallelize(ScalaData.sampleData())
    
    rdd.filter(x => x.age < 20).collect().foreach(println)
    
    rdd.filter(x => x.age > 20).collect().foreach(println)
    
  }
  
}