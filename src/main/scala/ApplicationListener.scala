import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerStageSubmitted, SparkListenerTaskStart, SparkListenerApplicationEnd}

import EventManager.EventManager

object ApplicationListener {
  def main(args:Array[String]):Unit= {

    val conf = new SparkConf().set("spark.extraListeners", "EventManager.EventManager")
    val sc = new SparkContext(conf)
    println(s"The application ID is (printed from the main() driver function is): ${sc.applicationId}")

    sc.setLogLevel("ERROR")

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._
    val person = Seq(
      ("John", "Barcelona"),
      ("Naveen", "Texas"),
      ("Rahul", "Bangalore")
    ).toDF("Name", "City")

    val city = Seq(
      ("Barcelona", "Spain", "Euro"),
      ("Bangalore", "India", "INR")
    ).toDF("City", "Country", "Currency")

    person.join(
      city,
      person("city") <=> city("city")
    ).show()
  }
}