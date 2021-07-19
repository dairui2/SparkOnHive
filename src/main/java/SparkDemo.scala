import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark to Hive")
      .master("yarn")
      .config("hive.metastore.uris", "thrift://hdp03:9083,thrift://hdp04:9083,thrift://hdp05:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.table("ods.test3")
    df.show(false)

    spark.close()
  }
}
