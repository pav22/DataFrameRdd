package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameJoins {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("DataFrameJoins").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val SqlCon = new SQLContext(sc)
    
    val leftData = SqlCon.createDataFrame(sc.parallelize
        (List(Numbers(1,"One"),Numbers(2,"Two"),Numbers(3,"Three"))))
        
    val rightData = SqlCon.createDataFrame(sc.parallelize
        (List(Numbers(4,"Four"),Numbers(5,"Five"),Numbers(3,"Three"))))
    
     
    val join = leftData.join(rightData,leftData.col("id").equalTo(rightData.col("id")))
    
    join.show()
    
    
       
        
  }
  
  
}

case class Numbers(id : Int, name : String)