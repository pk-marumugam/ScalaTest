import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SampleSpec extends AnyFlatSpec {
  val spark = SparkSession.builder.appName("Hello")
      .master("local")
      .getOrCreate()
  behavior of "Spark Transformer"

  it should("replace null values with unknown") in {
    val df = spark.read.option("header","true").csv("src/test/resources/data.csv")
    df.show(false)
    val transDf = Hello.transform(df)
    transDf.show(false)
    val genderList = transDf.filter("Name='Claire'")
      .select("Gender")
      .take(1).mkString(",")

    val gender = genderList
    println(s"Gender ${gender}")
    assert("[Unknown]"== gender)
  }


}
