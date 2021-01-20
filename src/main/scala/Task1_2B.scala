import org.apache.spark.sql.SparkSession

object Task1_2B extends App {

  val spark = SparkSession
    .builder()
    .appName("Groceries Analysis")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  val groceriesRDD = sc.textFile("src/main/resources/data/groceries.csv")

  val allGroceries = groceriesRDD.flatMap(row => row.split(","))
    .distinct()

  val totalNoOfGroceries = allGroceries.count()
  println(totalNoOfGroceries)

  //allGroceries.coalesce(1).saveAsTextFile("out/task1_2a")

}
