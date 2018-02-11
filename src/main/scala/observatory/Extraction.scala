package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 1st milestone: data extraction
  */
object Extraction {

  val sc: SparkContext = new SparkContext("local", "extraction", new SparkConf())

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationRDD: RDD[(String, (String, String))] = sc.textFile(getPath(stationsFile))
      .map(_.split(",").to[List])
      .filter(_.lengthCompare(4) == 0)
      .filter(l => !l.head.isEmpty || !l(1).isEmpty || !l(2).isEmpty || !l(3).isEmpty)
      .map(stationRow)

    val temperatureRDD: RDD[(String, (String, String, String, String))] = sc.textFile(getPath(temperaturesFile))
      .map(_.split(",").to[List])
      .filter(_.lengthCompare(5) == 0)
      .filter(l => !l.head.isEmpty && !l(1).isEmpty)
      .filter(l => l.last.toDouble != 9999.9)
      .map(temperatureRow(_, year))

    stationRDD.join(temperatureRDD).map(p =>
      (
        LocalDate.parse(s"${p._2._2._1}-${p._2._2._2}-${p._2._2._3}"),
        Location(p._2._1._1.toDouble, p._2._1._2.toDouble),
        getCelsius(p._2._2._4.toDouble)
      )
    ).collect()
  }

  private def getPath(localFile: String) = Paths.get(getClass.getResource(localFile).toURI).toString

  private def getCelsius(fahrenheit: Double): Temperature = (fahrenheit - 32) / 1.8

  private def stationRow(line: List[String]): (String, (String, String)) = (line.head + line(1), (line(2), line(3)))

  private def temperatureRow(line: List[String], year: Year): (String, (String, String, String, String)) = (line.head +
    line(1), (year.toString, line(2), line(3), line.last))

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.par
      .groupBy(_._2)
      .mapValues(t =>
        t.foldLeft(0.0)((sum, t) => sum + t._3) / t.size)
      .seq
  }
}
