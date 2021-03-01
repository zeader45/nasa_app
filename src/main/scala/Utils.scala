import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{to_date, udf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkContext, SparkFiles}

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.Pattern
import scala.util.control.Exception.allCatch

object Utils {

  def downFileFromFTP(sc: SparkContext, ftpUrl: String): RDD[String] = {

    val dataSource = ftpUrl
    sc.addFile(dataSource)
    val fileName = SparkFiles.get(dataSource.split("/").last)
    val file = sc.textFile(fileName)
    file
  }
}
