import org.apache.spark._
import SparkContext._
object SparkJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Application")
		val sc = new SparkContext(conf)
   
    // Read rating from HDFS file
    val ratings_url = "hdfs://localhost:9000/ml-1m/ratings.dat"
    val movies_url = "hdfs://localhost:9000/ml-1m/movies.dat"
    val dirOut = "hdfs://localhost:9000/movies_output"
    val textFile = sc.textFile(ratings_url)
 
    
    println("开始：")
    // ratings.txt
    //      0        1           2            3
    // userid::movieid::rating::timstamp
    //extract (movieid, rating)
    val rating = textFile.map(line => {
        val fileds = line.split("::")
        (fileds(1).toInt, fileds(2).toDouble)
       })
     println("************************"+rating.collect())
    // 得到 (movieid,avg)
    val movieScores = rating
       .groupByKey()
       .map(data => {
         val avg = data._2.sum / data._2.size
         (data._1, avg)
       })
     println("************************"+movieScores.collect())
     // Read movie from HDFS file
     val movies = sc.textFile(movies_url)
     val movieskey = movies.map(line => {
       val fileds = line.split("::")
        (fileds(0).toInt, fileds(1))
     }).keyBy(tup => tup._1)
 
     // by join, we get <movie, averageRating, movieName>
     val result = movieScores
       .keyBy(tup => tup._1)
       .join(movieskey)
       .filter(f => f._2._1._2 > 4.0)
       .map(f => (f._1, f._2._1._2, f._2._2._2))
    println("************************"+result)
    println("结束：")
    result.saveAsTextFile(dirOut)
  }
}