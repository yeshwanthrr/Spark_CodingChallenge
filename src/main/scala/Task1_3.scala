import org.apache.spark.sql.SparkSession

object Task1_3 extends App {

  val spark = SparkSession
    .builder()
    .appName("Groceries Analysis")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  val groceriesRDD = sc.textFile("src/main/resources/data/groceries.csv")

  val count = groceriesRDD.flatMap(row => row.split(","))
    .map(product => (product,1))
    .reduceByKey(_ + _)
    .map(row => (row._2,row._1))
    .sortByKey(false,1)
    .map(row => (row._2,row._1))
    .take(5)
    .foreach(println)


    //.take(5).foreach(println)

  //allGroceries.coalesce(1).saveAsTextFile("out/task1_2a")

}
