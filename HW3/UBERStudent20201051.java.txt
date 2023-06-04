import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.time.DayOfWeek
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.TextStyle
import java.util.Locale

object UBERStudent20201051 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UBERStudent20201051")
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))

    val result = data.map(line => {
      val parts = line.split(",")
      val baseNumber = parts(0)
      val dateString = parts(1)
      val activeVehicles = parts(2).toInt
      val trips = parts(3).toInt

      val date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("M/d/yyyy"))
      val dayOfWeek = date.getDayOfWeek
      var dayOfWeekString = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase

      if (dayOfWeekString.equals("THU")) {
        dayOfWeekString = "THR"
      }

      ((baseNumber, dayOfWeekString), (activeVehicles, trips))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    val formattedResult = result.map {
      case ((baseNumber, dayOfWeekString), (totalVehicles, totalTrips)) =>
        s"$baseNumber,$dayOfWeekString,$totalVehicles,$totalTrips"
    }

    formattedResult.saveAsTextFile(args(1))

    sc.stop()
  }
}
