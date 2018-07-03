package loganalyse

import java.sql.Date
import java.time.OffsetDateTime
import org.apache.spark.sql.Row

/**
  * this class represents an Apache access log line
  * see https://httpd.apache.org/docs/2.4/logs.html for more information's
  *
  * @param row
  */
@SerialVersionUID(15L)
class ApacheLogInfos(row: Row) extends Serializable {

  /* Row composition
   *
   * 0 - String: IP or name
   * 1 - String: Client ID on user machine
   * 2 - String: User name
   * 3 - OffsetDateTime: Date and time of request
   * 4 - String: Request Method
   * 5 - String: Request endpoint
   * 6 - String: Protocol
   * 7 - Integer: Response Code
   * 8 - Integer: Content size
   *
   */

  def getHostname: String = row.getString(0)

  def getID: String = row.getString(1)

  def getUserName: String = row.getString(2)

  def getDatetime: OffsetDateTime = row.getAs[OffsetDateTime](3)

  def getRequestMethod: String = row.getString(4)

  def getRequestEndpoint: String = row.getString(5)

  def getProtocol: String = row.getString(6)

  def getResponseCode: Int = row.getInt(7)

  def getContentSize: Int = row.getInt(8)


}
