import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOnHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("testApp")
      .config("spark.sql.warehouse.dir","hdfs://hdp04:8020/warehouse/tablespace/managed/hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()
    spark.sql("select * from ods.test3").show()
    var dataFrame: DataFrame = spark.table("ods.test3")

    //    dataFrame.sqlContext.sql("select * from ods.test3 where idd>=4")  //过滤器与下面一行二选一
    dataFrame.filter("idd >= 7 ")  //过滤器与上面一行二选一
    .write.saveAsTable("ods.test13")
    spark.stop()
  }
}
