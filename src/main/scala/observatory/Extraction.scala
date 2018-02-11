package observatory

import java.nio.file.Paths
import java.sql.Date
import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * 1st milestone: data extraction
  */
object Extraction {

  val spark: SparkSession = SparkSession.builder().appName("extraction").config("spark.master", "local").getOrCreate()

  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationRDD: RDD[Row] = spark.sparkContext.textFile(getPath(stationsFile))
      .map(_.split(",").to[List])
      .filter(_.lengthCompare(4) == 0)
      .filter(l => !l.head.isEmpty || !l(1).isEmpty || !l(2).isEmpty || !l(3).isEmpty)
      .map(stationRow)
    val stationDF = spark.createDataFrame(stationRDD, stationSchema)

    val year = getYear(temperaturesFile)
    val temperatureRDD: RDD[Row] = spark.sparkContext.textFile(getPath(temperaturesFile))
      .map(_.split(",").to[List])
      .filter(_.lengthCompare(5) == 0)
      .map(temperatureRow(_, year))
    val temperatureDF = spark.createDataFrame(temperatureRDD, temperatureSchema)

    val joinedDF = stationDF.join(temperatureDF, "id")

    val result = joinedDF.select("*").map(r =>
      (
        Date.valueOf(s"${r.get(3)}-${r.get(4)}-${r.get(5)}"),
        Location(r.get(1).toString.toDouble, r.get(2).toString.toDouble),
        getCelsius(r.get(6).toString.toDouble)
      )
    ).collect()

    result.map(t => (t._1.toLocalDate, t._2, t._3))
  }

  private def getPath(localFile: String) = Paths.get(getClass.getResource(localFile).toURI).toString

  private def getYear(str: String): Year = {
    val pattern = "\\d+".r
    pattern.findFirstIn(str).get.toInt
  }

  private def getCelsius(fahrenheit: Double): Temperature = (fahrenheit - 32) / 1.8

  private def stationRow(line: List[String]): Row = Row(line.head + line(1), line(2), line(3))

  private def stationSchema: StructType = StructType("id latitude longitude".split(" ").map(s => StructField(s,
    StringType, nullable = false)))

  private def temperatureRow(line: List[String], year: Year): Row = Row(line.head + line(1), year.toString, line(2), line(3),
    line.last)

  private def temperatureSchema: StructType = StructType("id year month day temperature".split(" ")
    .map(s => StructField(s, StringType, nullable = false)))

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.map(t => (t._2, t._3)).groupBy(_._1)
      .map { case (k, v) =>
        (k, v.map(_._2).sum / v.size)
      }
  }
}
