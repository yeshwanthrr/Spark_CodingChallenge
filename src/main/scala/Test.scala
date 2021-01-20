import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Test extends App {

  val spark = SparkSession.builder()
    .appName("Test App")
    .config("spark.master","local[2]")
    .getOrCreate()

  val dataSet = spark.sparkContext.parallelize(1 to 100)
  val result = dataSet.collect()
  dataSet.take(5).map(println(_))

}
