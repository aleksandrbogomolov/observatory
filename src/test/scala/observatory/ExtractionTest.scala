package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {

  val iterable = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")

  val averageTemp = Extraction.locationYearlyAverageRecords(iterable)

  averageTemp.foreach(println)
}
