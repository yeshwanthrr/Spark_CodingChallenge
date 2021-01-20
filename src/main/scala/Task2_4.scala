import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min}

object Task2_4 extends App {

  val spark = SparkSession
    .builder()
    .appName("Groceries Analysis")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val airbnbDF = spark.read.parquet("src/main/resources/data/sf-airbnb-clean.parquet")

  airbnbDF.show(5)
  //airbnbDF.printSchema()

  val filterCondition = airbnbDF
    .select(min(col("price")).alias("lowest_price"),
      max(col("review_scores_rating")).alias("max_review"))
    .head()

  val lowest_price = filterCondition.get(0)
  val max_review = filterCondition.get(1)

  airbnbDF.
    where(s"price = ${lowest_price} and review_scores_rating = ${max_review}")
    .select(col("accommodates"),col("price"),col("review_scores_rating"))
    .show()







//
//  result.write
//    .format("csv")
//    .option("header",true)
//    .option("sep",",")
//    .save("out/task2_3.txt")

}
