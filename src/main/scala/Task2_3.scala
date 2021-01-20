import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, max, min}

object Task2_3 extends App {

  val spark = SparkSession
    .builder()
    .appName("Groceries Analysis")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val airbnbDF = spark.read.parquet("src/main/resources/data/sf-airbnb-clean.parquet")

  airbnbDF.printSchema()

  val result = airbnbDF
    .where("price > 5000 and review_scores_value = 10")
    .select(
      avg(col("bathrooms")).alias("avg_bathrooms"),
      avg(col("bedrooms")).alias("avg_bathrooms"))


  result.show()


//
//  result.write
//    .format("csv")
//    .option("header",true)
//    .option("sep",",")
//    .save("out/task2_3.txt")

}
