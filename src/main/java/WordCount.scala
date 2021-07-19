import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}

object WordCount {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val inputFile =  "file:///Users/dai/Downloads/test2.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}

//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//
//object WordCount {
//
//  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "root")
//    System.setProperty("user.name", "root")
//
//    val conf = new SparkConf().setAppName("WordCount").setMaster("yarn")
//      .set("deploy-mode", "client")
//      .set("spark.yarn.jars", "hdfs://hdp04:8020/warehouse/tablespace/managed/hive/*")  //集群的jars包,是你自己上传上去的
////      .set("spark.yarn.jars", "/usr/hdp/3.1.4.0-315/spark2/examples/jars/*")
//      .setJars(List("/usr/hdp/3.1.4.0-315/spark2/examples/jars/spark-examples_2.11-2.3.2.3.1.4.0-315.jar")) //这是sbt打包后的文件
//      .setIfMissing("spark.driver.host", "116.196.88.54") //设置你自己的ip
//
//    val sc = new SparkContext(conf)
//
//    val rdd = sc.textFile("hdfs://hdp04:8020/warehouse/tablespace/managed/hive/ods.db/test3/test2.txt")
////    val count = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
////    count.collect().foreach(println)
//  }
//}