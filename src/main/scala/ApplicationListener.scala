import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerStageSubmitted, SparkListenerTaskStart, SparkListenerApplicationEnd}

import EventManager.EventManager

object ApplicationListener {
  def main(args:Array[String]):Unit= {
    
    val sc = new SparkContext()
    val em = new EventManager.EventManager
    em.appID = sc.applicationId
    em.appName = sc.appName
    em.HTTP_endpoint = args(0)
    sc.addSparkListener(em)

    println(s"\n\n\nThe application ID is driver function is): ${sc.applicationId} and the HTTP end point is ${args(0)}\n\n\n")

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