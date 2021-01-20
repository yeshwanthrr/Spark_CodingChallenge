import org.apache.spark.sql.SparkSession

object Task1_1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Groceries Analysis")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  val groceriesRDD = sc.textFile("src/main/resources/data/groceries.csv")

  groceriesRDD.map(x => List(x))
    .take(5)
    .foreach(println)

}
