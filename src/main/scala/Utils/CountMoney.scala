package Utils

object CountMoney {
  def change(list: List[(Any)]): List[(String, Double, Double)] = {
    // 创建一个空集合
    var list1 = List[(String, Double, Double)]()
    // 切分数据，因为我们统计需要的小时
    val str = list(0).toString.substring(8)
    var st = 0
    // 判断一下时间段
    if (str.substring(0, 1).equals("0")) {
      st = ("8" + str).toInt
    } else {
      st = str.toInt
    }
    // 进行匹配
    if (st.toString.toInt >= 80000 && st.toString.toInt <= 80059) {
      list1 :+= ("2017041200", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80100 && st.toString.toInt <= 80159) {
      list1 :+= ("2017041201", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80200 && st.toString.toInt <= 80259) {
      list1 :+= ("2017041202", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80300 && st.toString.toInt <= 80359) {
      list1 :+= ("2017041203", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80400 && st.toString.toInt <= 80459) {
      list1 :+= ("2017041204", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80500 && st.toString.toInt <= 80559) {
      list1 :+= ("2017041205", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80600 && st.toString.toInt <= 80659) {
      list1 :+= ("2017041206", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80700 && st.toString.toInt <= 80759) {
      list1 :+= ("2017041207", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80800 && st.toString.toInt <= 80859) {
      list1 :+= ("2017041208", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 80900 && st.toString.toInt <= 80959) {
      list1 :+= ("2017041209", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1000 && st.toString.toInt <= 1059) {
      list1 :+= ("2017041210", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1100 && st.toString.toInt <= 1159) {
      list1 :+= ("2017041211", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1200 && st.toString.toInt <= 1259) {
      list1 :+= ("2017041212", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1300 && st.toString.toInt <= 1359) {
      list1 :+= ("2017041213", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1400 && st.toString.toInt <= 1459) {
      list1 :+= ("2017041214", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1500 && st.toString.toInt <= 1559) {
      list1 :+= ("2017041215", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1600 && st.toString.toInt <= 1659) {
      list1 :+= ("2017041216", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1700 && st.toString.toInt <= 1759) {
      list1 :+= ("2017041217", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1800 && st.toString.toInt <= 1859) {
      list1 :+= ("2017041218", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 1900 && st.toString.toInt <= 1959) {
      list1 :+= ("2017041219", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 2000 && st.toString.toInt <= 2059) {
      list1 :+= ("2017041220", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 2100 && st.toString.toInt <= 2159) {
      list1 :+= ("2017041221", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 2200 && st.toString.toInt <= 2259) {
      list1 :+= ("2017041222", list(1).toString().toDouble, list(2).toString().toDouble)
    }
    if (st.toString.toInt >= 2300 && st.toString.toInt <= 2359) {
      list1 :+= ("2017041223", list(1).toString().toDouble, list(2).toString().toDouble)
    }

    list1
  }
}
