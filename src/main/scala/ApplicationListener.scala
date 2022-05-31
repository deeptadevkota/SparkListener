import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerStageSubmitted, SparkListenerTaskStart, SparkListenerApplicationEnd}

import EventManager.EventManager

object ApplicationListener {
  def main(args:Array[String]):Unit= {

    val sc = new SparkContext()


    var em: EventManager = null

    if(args.length == 0)
    {
      em = new EventManager(sc.applicationId, sc.appName)
    }
    else
    {
      em = new EventManager(sc.applicationId, sc.appName, args(0))
    }
    sc.addSparkListener(em)

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