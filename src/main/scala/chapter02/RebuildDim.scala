package chapter02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col

/**
 * create 2020-10-25
 * author zy
 * desc 重建维度
 */
object RebuildDim {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("first").getOrCreate()
    val group = Grouped(Array("USA","France","GB"),Array("Barack Obama","David","Francois Hoolande"))
    val ds = spark.sparkContext.parallelize(Seq(group))  //create dataframe
    val df = spark.createDataFrame(ds)
    //explode
    df.withColumn("locations",explode(col("locations"))).show(false)
  }
}

case class Grouped(locations:Array[String],people:Array[String])
