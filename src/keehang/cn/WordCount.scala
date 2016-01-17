import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
/**
 * 统计输入目录中所有单词出现的总次数
 */
object WordCount {
	def main(args: Array[String]) {
		println("开始执行：")
	  val dirIn = "hdfs://localhost:9000/NET.txt"
		val dirOut = "hdfs://localhost:9000/output"
/*		Master URL 含义
      local 使用一个Worker线程本地化运行SPARK(完全不并行)
      local[*]使用逻辑CPU个数数量的线程来本地化运行Spark
      local[K]使用K个Worker线程本地化运行Spark（理想情况下，K应该根据运行机器的CPU核数设定）
      spark://HOST:PORT连接到指定的Spark standalone master。默认端口是7077.
      yarn-client以客户端模式连接YARN集群。集群的位置可以在HADOOP_CONF_DIR 环境变量中找到。
      yarn-cluster 以集群模式连接YARN集群。集群的位置可以在HADOOP_CONF_DIR 环境变量中找到。
      mesos://HOST:PORT 连接到指定的Mesos集群。默认接口是5050.
      而spark-shell会在启动的时候自动构建SparkContext，名称为sc。*/
		val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val line = sc.textFile(dirIn)
		//val cnt = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _) // 文件按空格拆分，统计单词次数
		//val sortedCnt = cnt.map(x => (x._2, x._1)).sortByKey(ascending = false).map(x => (x._2, x._1)) // 按出现次数由高到低排序
		//sortedCnt.collect().foreach(println) // 控制台输出
		//sortedCnt.saveAsTextFile(dirOut) // 写入文本文件
		val filterline = line.filter(_.contains("www.baidu.com"))
		filterline.cache()
		val result = filterline.count()
		println(result)
		sc.stop()
	}
}