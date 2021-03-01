
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.functions.{col, concat, regexp_replace, split, substring, to_date, udf}
import org.apache.spark.sql._
import scala.reflect.io.Directory
import java.io.File


object NasaDataProcessor {

  import org.apache.log4j.{Level, Logger}
  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "C:\\Users\\zeade\\IdeaProjects\\winutils\\hadoop-2.7.1")

    val conf: SparkConf = new SparkConf().setAppName("Nasa App").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    import spark.implicits._

    // Prepend "anonymous:{randomstring}" to the url so it can access the FTP file
    val nasaFile = Utils.downFileFromFTP(sc, "ftp://anonymous:11@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz")

    // Encounter fileNotFound error when directly converting rdd to DF. Have to save rdd to text file in local first and read from there.
    // Delete existing local files otherwise saveAsTextFile will throw an error.
    val dir = new Directory(new File("/opt/downloaded/"))
    dir.deleteRecursively()

    nasaFile.saveAsTextFile("/opt/downloaded/nasaFile")
    val nasaFileDownloaded = spark.read.text("/opt/downloaded/nasaFile")

    val result = nasaFileDownloaded.withColumn("IPAddress", split(col("value"), " - - ")(0)).
      withColumn("temp1", split(col("value"), " - - ")(1)).
      withColumn("Time", concat(split(col("temp1"), " ")(0), split(col("temp1"), " ")(1))).
      withColumn("dateTime", regexp_replace($"Time", "\\[", "")).
      withColumn("date", to_date($"dateTime", "dd/MMM/yyyy:HH:mm:ssX")).
      withColumn("method", substring(split(col("temp1"), " ")(2), 2, 3)).
      withColumn("url", split(col("temp1"), " ")(3)).
      select("IPAddress", "date", "dateTime", "method", "url")

    result.createOrReplaceTempView("metricView")
    val number = args(0).toInt

    //print topN most frequent visitors in date ascending order
    val sql =
      s"""
         |with cnt_visitors as (
         |select date, IPAddress as visitors, count(*) as cnt
         |from metricView
         |where date is not null
         |group by date, IPAddress)
         |
         |, cte as (
         |select date, visitors, dense_rank() over (partition by date order by cnt desc) as rnk
         |from cnt_visitors)
         |
         |select date, visitors, rnk from cte
         |where rnk <= ${number}
         |order by date, rnk
         |""".stripMargin

    val topNVisitors = spark.sql(sql)
    topNVisitors.show(100, false)


    //print topN most frequent urls in date ascending order
    val sql2 =
      s"""
         |with cnt_url as (
         |select date, url, count(*) as cnt
         |from metricView
         |where date is not null
         |group by date, url)
         |
         |, cte as (
         |select date, url, dense_rank() over (partition by date order by cnt desc) as rnk
         |from cnt_url)
         |
         |select date, url, rnk from cte
         |where rnk <= ${number}
         |order by date, rnk
         |""".stripMargin

    val topNUrl = spark.sql(sql2)
    topNUrl.show(200, false)

  }
}