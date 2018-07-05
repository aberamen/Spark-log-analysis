package loganalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.format.TextStyle
import java.util.Locale


object LogAnalyseFuns {

  def getParsedData(data: RDD[String]): (RDD[(Row, Int)], RDD[ApacheLogInfos], RDD[Row]) = {

    val parsed_logs = data.map(Utilities.parse_line(_))
    val access_logs = parsed_logs.filter(_._2 == 1).map(row => new ApacheLogInfos(row._1)).cache()
    val failed_logs = parsed_logs.filter(_._2 == 0).map(_._1)
    (parsed_logs, access_logs, failed_logs)
  }

  def calculateLogStatistic(data: RDD[ApacheLogInfos]): (Long, Long, Long) = {

    val contentSize = data.map[Long](f => f.getContentSize)

    (contentSize.min(),
      contentSize.max(),
      (contentSize.sum() / contentSize.count()).toLong)


  }

  /*
   * Calculate for the content size the following values:
   * 
   * minimum: Minimum value
   * maximum: Maximum value
   * average: Average
   * 
   * Return the following triple:
   * (min,max,avg)
   */


  def getResponseCodesAndFrequencies(data: RDD[ApacheLogInfos]): List[(Int, Int)] = {
    data.map(f => (f.getResponseCode, 1)).reduceByKey((x, y) => x + y).collect().toList
  }

  /* 
   * Calculate for each single response code the number of occurences
   * Return a list of tuples which contain the response code as the first
   * element and the number of occurences as the second.
   *
   */


  def get20HostsAccessedMoreThan10Times(data: RDD[ApacheLogInfos]): List[String] = {
    data.map(f => (f.getHostname, 1)).reduceByKey((x, y) => x + y).filter(x => x._2 > 10).keys.distinct().take(20).toList
  }

  /* 
   * Calculate 20 abitrary hosts from which the web server was accessed more than 10 times 
   * Print out the result on the console (no tests take place)
   */

  def getTopTenEndpoints(data: RDD[ApacheLogInfos]): List[(String, Int)] = {
    data.map(f => (f.getRequestEndpoint, 1)).reduceByKey((x, y) => x + y).collect().toList.sortBy(_._2)(Ordering[Int].reverse).take(10)
  }

  /* 
   * Calcuclate the top ten endpoints. 
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of accesses as the second
   * The list should be ordered by the number of accesses.
   */

  def getTopTenErrorEndpoints(data: RDD[ApacheLogInfos]): List[(String, Int)] = {
    data.filter(x => x.getResponseCode != 200)
      .groupBy(x => x.getRequestEndpoint)
      .map(x => (x._1, x._2.count(c => true)))
      .sortBy(x => x._2, ascending = false)
      .collect()
      .toList
     // .sortBy(_._2)(Ordering[Int].reverse)
      .take(10)

  }

  /* 
   * Calculate the top ten endpoint that produces error response codes (response code != 200).
   * 
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of errors as the second.
   * The list should be ordered by the number of accesses.
   */

  def getNumberOfRequestsPerDay(data: RDD[ApacheLogInfos]): List[(Int, Int)] = {
    data.groupBy(f => f.getDatetime.getDayOfMonth)
      .map(x => (x._1, x._2.count(c => true)))
      .sortBy(x => x._1, ascending = true)
      .collect()
      .toList
  }

  /* 
   * Calculate the number of requests per day.
   * Return a list of tuples which contain the day (1..30) as the first element and the number of
   * accesses as the second.
   * The list should be ordered by the day number.
   */

  def numberOfUniqueHosts(data: RDD[ApacheLogInfos]): Long = {
    data.groupBy(x => x.getHostname).distinct().count()
  }

  /* 
   * Calculate the number of hosts that accesses the web server in June 95.
   * Every hosts should only be counted once.
   */

  def numberOfUniqueDailyHosts(data: RDD[ApacheLogInfos]): List[(Int, Int)] = {
    data.groupBy(f => f.getDatetime.getDayOfMonth)
      .map(x => (x._1, x._2.map(n => n.getHostname).toList.distinct.count(c => true)))
      .sortBy(x => x._1, ascending = true)
      .collect()
      .toList
  }

  /* 
   * Calculate the number of hosts per day that accesses the web server.
   * Every host should only be counted once per day.
   * Order the list by the day number.
   */

  def averageNrOfDailyRequestsPerHost(data: RDD[ApacheLogInfos]): List[(Int, Int)] = {
    data.groupBy(f => f.getDatetime.getDayOfMonth)
      .map(x => (x._1, x._2.size / x._2.map(n => n.getHostname).toList.distinct.count(c => true)))
      .sortBy(x => x._1, ascending = true)
      .collect()
      .toList
  }

  /*
   * Calculate the average number of requests per host for each single day.
   * Order the list by the day number.
   */

  def top25ErrorCodeResponseHosts(data: RDD[ApacheLogInfos]): Set[(String, Int)] = {
    data.groupBy(f => f.getHostname)
      .mapValues(x => x.filter(x => x.getResponseCode == 404).size)
      .sortBy(x => x._2, ascending = false)
      .take(25)
      .toSet
  }

  /*
   * Calculate the top 25 hosts that causes error codes (Response Code=404)
   * Return a set of tuples consisting the hostnames  and the number of requests
   */

  def responseErrorCodesPerDay(data: RDD[ApacheLogInfos]): List[(Int, Int)] = {
    data.groupBy(f => f.getDatetime.getDayOfMonth)
      .mapValues(x => x.filter(x => x.getResponseCode == 404).size)
      .sortBy(x => x._1, ascending = true)
      .collect()
      .toList
  }

  /*
   * Calculate the number of error codes (Response Code=404) per day.
   * Return a list of tuples that contain the day as the first element and the number as the second. 
   * Order the list by the day number.
   */

  def errorResponseCodeByHour(data: RDD[ApacheLogInfos]): List[(Int, Int)] = {
    data.groupBy(f => f.getDatetime.getHour)
      .mapValues(x => x.filter(x => x.getResponseCode == 404).size)
      .sortBy(x => x._1, ascending = true)
      .collect()
      .toList
  }

  /*
   * Calculate the error response coded for every hour of the day.
   * Return a list of tuples that contain the hour as the first element (0..23) abd the number of error codes as the second.
   * Ergebnis soll eine Liste von Tupeln sein, deren erstes Element die Stunde bestimmt (0..23) und 
   * Order the list by the hour-number.
   */


  def getAvgRequestsPerWeekDay(data: RDD[ApacheLogInfos]): List[(Int, String)] = {
    data.groupBy(f => f.getDatetime.getDayOfWeek.getValue)
      .mapValues(x => x.size / x.map(_.getDatetime.toLocalDate).toList.distinct.size)
      .map(x => (x._2, x._1))
      .sortBy(x => x._2, ascending = true)
      .mapValues{
        x => x  match {
          case 1 => "Monday"
          case 2 => "Tuesday"
          case 3 => "Wednesday"
          case 4 => "Thursday"
          case 5 => "Friday"
          case 6 => "Saturday"
          case 7 => "Sunday"
        }
      }
      .collect()
      .toList
  }

  /*
   * Calculate the number of requests per weekday (Monday, Tuesday,...).
   * Return a list of tuples that contain the number of requests as the first element and the weekday
   * (String) as the second.
   * The elements should have the following order: [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday].
   */
}
