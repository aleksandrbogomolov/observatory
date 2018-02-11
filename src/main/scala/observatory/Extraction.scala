package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 1st milestone: data extraction
  */
object Extraction {

  val sc: SparkContext = new SparkContext("local[*]", "extraction", new SparkConf())

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationRDD: RDD[((String, String), Location)] = sc.textFile(getPath(stationsFile))
      .map(_.split(",").to[List])
      .filter(l => l.size == 4 && !l(2).isEmpty && !l(3).isEmpty)
      .map(stationRow)

    val temperatureRDD: RDD[((String, String), (LocalDate, Temperature))] = sc.textFile(getPath(temperaturesFile))
      .map(_.split(",").to[List])
      .filter(_.size == 5)
      .filter(l => l.last.toDouble != 9999.9)
      .map(temperatureRow(_, year))

    stationRDD.join(temperatureRDD).map(p => (p._2._2._1, p._2._1, getCelsius(p._2._2._2))).collect()
  }

  private def getPath(localFile: String) = Paths.get(getClass.getResource(localFile).toURI).toString

  private def getCelsius(fahrenheit: Double): Temperature = (fahrenheit - 32) / 1.8

  private def stationRow(line: List[String]): ((String, String), Location) = ((line.head, line(1)), Location(line(2).toDouble, line(3).toDouble))

  private def temperatureRow(line: List[String], year: Year): ((String, String), (LocalDate, Temperature)) =
    ((line.head, line(1)), (LocalDate.of(year, line(2).toInt, line(3).toInt), line.last.toDouble))

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.par.groupBy(_._2)
      .mapValues(
        l => l.foldLeft(0.0)(
          (t, r) => t + r._3) / l.size
      ).seq
  }
}
