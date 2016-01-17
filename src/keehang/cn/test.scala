import scala.io.Source
import org.apache.spark._
import SparkContext._
object test {
   def main(args: Array[String]): Unit = {
     
     //print(System.getenv("SPARK_HOME"))
      val conf = new SparkConf().setMaster("local[*]").setAppName("Application")
		  val sc = new SparkContext(conf)
      val rdd = sc.makeRDD(1 to 10 , 100).repartition(4)
      println(rdd.partitions.size)
   }
}