package Utils

import java.text.SimpleDateFormat

object TimeUtils {
  def changeTime(startTime:String,receiveTime:String):Double={
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val starttime = format.parse(startTime.substring(0,17)).getTime
    val endtime = format.parse(receiveTime).getTime

    endtime - starttime
  }
}
