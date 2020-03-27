package retail_db

import java.util.Date
import java.text.SimpleDateFormat

object Test extends App {
  new Date()
  //2013-07-25 00:00:00.0
  var sDate1 = "2013-07-25 00:00:00.0";
  println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(sDate1).getTime())
}