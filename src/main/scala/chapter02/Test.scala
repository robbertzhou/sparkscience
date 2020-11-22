package chapter02

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame

object Test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName("test").getOrCreate()
    createTransportGraph(sparkSession)
  }

  def createTransportGraph(spark:SparkSession): Unit ={
    //nodes
    val nodeSchema = StructType(
      List(
        StructField("id",DataTypes.StringType,true),
        StructField("latitude",DataTypes.FloatType,true),
        StructField("longitude",DataTypes.FloatType,true),
        StructField("population",DataTypes.IntegerType,true)
      )
    )

    val frame = spark.read.option("header","true").csv("i:\\testdata\\transport-nodes.csv")
    val rdd = frame.rdd
    val converted = rdd.map(line =>{
      Row(
        line.get(0),
        line.get(1).toString.toFloat,
        line.get(2).toString.toFloat,
        line.get(3).toString.toInt
      )
    })
    val df = spark.createDataFrame(converted,nodeSchema)

    //relationships
    val rels = spark.read.option("header","true").csv("i:\\testdata\\transport-relationships.csv")
    val reversed_rels = rels.withColumn("newSrc",col = col("dst"))
        .withColumn("newDst",col("src"))
        .drop("src","dst")
        .withColumnRenamed("newSrc","src")
        .withColumnRenamed("newDst","dst")
    val relships = rels.union(reversed_rels)
    val gf = GraphFrame(df,relships)
    gf.vertices.show(false)
    gf.bfs()

  }
}
