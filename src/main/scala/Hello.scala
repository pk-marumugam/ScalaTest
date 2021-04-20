import org.apache.spark.sql.{DataFrame, SparkSession}

object Hello {
  def main(args: Array[String]):Unit ={
  val sp = SparkSession.builder()
    .master("local")
    .appName("Hello")
    .getOrCreate()
  println(s"Application Name: ${sp.sparkContext.appName}")
  val df = sp.read.option("header","true").option("inferSchema","true").csv("src/main/resources/data.csv")

  val tDf = transform(df)
  tDf.show(false)
  val genderList = tDf.filter("Name='Claire'")
      .select("Gender").take(1).mkString(",")

  println(genderList)
  }
  def transform(inputDf:DataFrame): DataFrame = {
    inputDf.na.fill("Unknown")
  }

}
