package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math.{atan2, pow, sqrt}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    import math.{cos, sin, toRadians}

    val p = 2
    val earthRadius = 6371

    def distance(from: Location, to: Location): Double = {
      val Location(latFrom, lonFrom) = from
      val Location(latTo, lonTo) = to
      val latDistance = toRadians(latTo - latFrom)
      val lonDistance = toRadians(lonTo - lonFrom)

      val a = pow(sin(latDistance / 2), 2) +
        cos(toRadians(latFrom)) * cos(toRadians(latTo)) *
          pow(sin(lonDistance / 2), 2)

      val c = 2 * atan2(sqrt(a), sqrt(1 - a))
      c * earthRadius
    }

    /**
      * Compute inverse distance weighting by applying Shepard's method.
      *
      * @return Interpolated value of temperature at the specific location.
      */
    def shepardMethod(): Double = {

      def weight(d: Double): Double = {
        1 / math.pow(d, p)
      }

      val et = temperatures.filter(_._1 == location)
      if (et.size == 1) et.head._2 else {
        val sums = temperatures.map {
          case (l, t) =>
            val dist = distance(location, l)
            if (dist < 1) return t
            val w = weight(dist)
            (w * t, w)
        }.reduce(
          (first, second) => (first._1 + second._1, first._2 + second._2)
        )
        sums._1 / sums._2
      }
    }

    shepardMethod()
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    /**
      * Note: Points must be sorted by temperature in ascending order.
      *
      * @param sortedPoints Sorted color points.
      * @return Two closest colors.
      */
    def findTwoClosestFromAscendingOrderedPoints(sortedPoints: Iterable[(Temperature, Color)], temperature: Temperature): ((Temperature, Color), (Temperature, Color)) = {
      val warmest = sortedPoints.last
      val coolest = sortedPoints.head
      if (warmest._1 <= temperature) (warmest, warmest)
      else if (coolest._1 >= temperature) (coolest, coolest)
      else sortedPoints.zip(sortedPoints.tail).filter {
        case ((t1, c1), (t2, c2)) => temperature >= t1 && temperature <= t2
      }.head
    }

    def interpolateColorIn(colderColor: (Temperature, Color), warmerColor: (Temperature, Color), value: Temperature): Color = {
      if (colderColor._2 == warmerColor._2) warmerColor._2
      else if (colderColor._1 == value) colderColor._2
      else if (warmerColor._1 == value) warmerColor._2
      else {
        val (t1, Color(r1, g1, b1)) = colderColor
        val (t2, Color(r2, g2, b2)) = warmerColor
        val r = interpolate(r1, r2, t1, t2, value).round.toInt
        val g = interpolate(g1, g2, t1, t2, value).round.toInt
        val b = interpolate(b1, b2, t1, t2, value).round.toInt
        Color(r, g, b)
      }
    }

    val filteredPoints = points.filter(_._1 == value)
    if (filteredPoints.size == 1)
      filteredPoints.toList.head._2
    else {
      val (colder, warmer) = findTwoClosestFromAscendingOrderedPoints(points, value)
      interpolateColorIn(colder, warmer, value)
    }
  }

  /**
    * Compute a linear interpolation between two color.
    *
    * @param sc Smaller color value.
    * @param bc Bigger color value.
    * @param st Smaller temperature.
    * @param bt Bigger temperature.
    * @param t  Specific temperature.
    * @return
    */
  def interpolate(sc: Int, bc: Int, st: Double, bt: Double, t: Double): Double = {
    sc * (1 - ((t - st) / (bt - st))) + bc * ((t - st) / (bt - st))
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    /**
      * Compute average temperature using predictTemperature()
      * Choose color by computed temperature using interpolateColor()
      * Collect all pixels' colors and create image (360x180).
      **/
    val rows = 180
    val columns = 360
    val pixels = Array.ofDim[Pixel](rows * columns)
    val colours = colors.toList.sortWith(_._1 < _._1)

    for {
      i <- 0 until rows
      j <- 0 until columns
    } yield {
      val location = Location(90 - i, j - 180)
      val temperature = predictTemperature(temperatures, location)
      val color = interpolateColor(colours, temperature)
      pixels((i * columns) + j) = Pixel.apply(color.red, color.green, color.blue, 127)

    }
    val image = Image(columns, rows, pixels)
    image
  }
}
