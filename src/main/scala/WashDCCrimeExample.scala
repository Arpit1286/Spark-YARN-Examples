import org.apache.spark.{SparkConf, SparkContext}

object RDDlabInApp {
  def main(args: Array[String]): Unit = {
    val inputFile = "/user/spark/wash_dc_crime_incidents_2013.csv"
    val conf = new SparkConf().setAppName("Notebook Exercise on YARN")
    val sc = new SparkContext(conf)

    // load the data
    val baseRDD = sc.textFile(inputFile)

    // remove the header
    val noHeaderRDD = baseRDD.filter {line => !(line.contains("REPORTDATETIME"))}


    // make RDD of scala objects
    val dataRDD = noHeaderRDD.map { line =>
      val cols = line.split(",")
      CrimeData(cols(0), cols(1), cols(2), cols(3), cols(4))
    }

    // cache for performance
    dataRDD.cache()

    // group the data by crime
    val groupedByOffenseRDD = dataRDD.groupBy {data => data.offense}

    // count the offenses
    val offenseCounts = groupedByOffenseRDD.map(g => (g._1, g._2.toSeq.length)).collect()
    for ((offense, count) <- offenseCounts) {
      println(f"$offense%30s $count%5d")
    }

    // number of homicides by weapon
    val homicidesByWeapon = dataRDD.filter(_.offense == "HOMICIDE").map(item => (item.method, 1)).reduceByKey(_ + _).collect()
    for ((method, count) <- homicidesByWeapon)
      println(f"$method%10s $count%d")

    // crime By Shifts
    val crimeByShift = dataRDD.map(item => (item.shift, 1)).reduceByKey((x,y) => x + y).map(line => (line._2, line._1)).max()
    println(crimeByShift)
  }

}

case class CrimeData(ccn: String,
                     reportTime: String,
                     shift: String,
                     offense: String,
                     method: String)

