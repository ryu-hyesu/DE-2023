import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object IMDBStudent20201051 {
  def main(args Array[String]) {
    val conf = new SparkConf().setAppName(IMDBStudent20201051)
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))

    val wordCounts = data
      .flatMap(line = line.split())
      .flatMap(token = token.split())
      .map(word = (word.trim, 1))
      .reduceByKey(_ + _)

    wordCounts.saveAsTextFile(args(1))

    sc.stop()
  }
}
