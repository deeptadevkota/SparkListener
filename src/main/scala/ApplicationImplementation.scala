package ApplicationImplementation

import org.apache.spark.sql.SparkSession


class ApplicationImplementation (val spark: SparkSession ) {

  def appCode() {
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
