import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, min}

object Task2_1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Groceries Analysis")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val airbnbDF = spark.read.parquet("src/main/resources/data/sf-airbnb-clean.parquet")

  val result = airbnbDF.select(
    min(col("price")).alias("min_price"),
    max(col("price")).alias("max_price"),
    count("*").alias("row_count")
  )

//  result.show()

  result.write
    .format("csv")
    .option("header",true)
    .option("sep",",")
    .save("out/task2_2.txt")

}
