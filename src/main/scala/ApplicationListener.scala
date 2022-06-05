import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerStageSubmitted, SparkListenerTaskStart, SparkListenerApplicationEnd}

import EventManager.EventManager
import ApplicationImplementation.ApplicationImplementation

// $SPARK_HOME/bin/spark-submit --class ApplicationListener --driver-java-options -DHTTP_endpoint=xyz --master local SparkListener.jar
object ApplicationListener {
  def main(args:Array[String]):Unit= {

    val sc = new SparkContext()
    var em: EventManager = null
    val HTTP_endpoint = System.getProperty("HTTP_endpoint")
    if(HTTP_endpoint ==null)
    {
      em = new EventManager(sc.applicationId, sc.appName)
    }
    else
    {
      em = new EventManager(sc.applicationId, sc.appName, HTTP_endpoint)
    }
    sc.addSparkListener(em)
    sc.setLogLevel("ERROR")

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Application with Listener")
      .getOrCreate()

    val appImplement = new ApplicationImplementation(spark)
    appImplement.appCode()
  }
}